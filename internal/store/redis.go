package store

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"github.com/redis/go-redis/v9"

	"github.com/sakshat-patil/edgefleet/internal/model"
)

// Redis key layout:
//
//	ef:device:{id}      string  JSON-encoded model.Device
//	ef:devices          set     all device IDs
//	ef:group:{name}     set     device IDs in a group
//	ef:cmds:{id}        list    pending commands for a device (RPUSH / LPOP)
//	ef:det:{id}         string  detection counter (INCRBY)
//	ef:rollout:{id}     string  JSON-encoded model.Rollout
//	ef:rollouts         set     all rollout IDs
type Redis struct {
	c *redis.Client
}

// NewRedis connects to addr (host:port).
func NewRedis(addr, password string) *Redis {
	return &Redis{c: redis.NewClient(&redis.Options{Addr: addr, Password: password, PoolSize: 64})}
}

func devKey(id string) string     { return "ef:device:" + id }
func groupKey(g string) string    { return "ef:group:" + g }
func cmdKey(id string) string     { return "ef:cmds:" + id }
func detKey(id string) string     { return "ef:det:" + id }
func rolloutKey(id string) string { return "ef:rollout:" + id }

func (r *Redis) Ping(ctx context.Context) error { return r.c.Ping(ctx).Err() }

func (r *Redis) PutDevice(ctx context.Context, d *model.Device) error {
	b, err := json.Marshal(d)
	if err != nil {
		return err
	}
	old, err := r.GetDevice(ctx, d.ID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	_, err = r.c.TxPipelined(ctx, func(p redis.Pipeliner) error {
		if old != nil && old.Group != d.Group {
			p.SRem(ctx, groupKey(old.Group), d.ID)
		}
		p.Set(ctx, devKey(d.ID), b, 0)
		p.SAdd(ctx, "ef:devices", d.ID)
		p.SAdd(ctx, groupKey(d.Group), d.ID)
		return nil
	})
	return err
}

func (r *Redis) GetDevice(ctx context.Context, id string) (*model.Device, error) {
	b, err := r.c.Get(ctx, devKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	d, err := clone[model.Device](b)
	if err != nil {
		return nil, err
	}
	if n, err := r.c.Get(ctx, detKey(id)).Int64(); err == nil {
		d.Detections = n
	}
	return d, nil
}

func (r *Redis) ListDevices(ctx context.Context, group string) ([]*model.Device, error) {
	set := "ef:devices"
	if group != "" {
		set = groupKey(group)
	}
	ids, err := r.c.SMembers(ctx, set).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(ids)
	out := make([]*model.Device, 0, len(ids))
	const batch = 500
	for i := 0; i < len(ids); i += batch {
		end := min(i+batch, len(ids))
		keys := make([]string, 0, 2*(end-i))
		for _, id := range ids[i:end] {
			keys = append(keys, devKey(id))
		}
		for _, id := range ids[i:end] {
			keys = append(keys, detKey(id))
		}
		vals, err := r.c.MGet(ctx, keys...).Result()
		if err != nil {
			return nil, err
		}
		n := end - i
		for j := 0; j < n; j++ {
			s, ok := vals[j].(string)
			if !ok {
				continue
			}
			d, err := clone[model.Device]([]byte(s))
			if err != nil {
				return nil, err
			}
			if c, ok := vals[n+j].(string); ok {
				var cnt int64
				_ = json.Unmarshal([]byte(c), &cnt)
				d.Detections = cnt
			}
			out = append(out, d)
		}
	}
	return out, nil
}

func (r *Redis) DeleteDevice(ctx context.Context, id string) error {
	d, err := r.GetDevice(ctx, id)
	if err != nil {
		return err
	}
	_, err = r.c.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Del(ctx, devKey(id), cmdKey(id), detKey(id))
		p.SRem(ctx, "ef:devices", id)
		p.SRem(ctx, groupKey(d.Group), id)
		return nil
	})
	return err
}

func (r *Redis) Groups(ctx context.Context) (map[string]int, error) {
	ds, err := r.ListDevices(ctx, "")
	if err != nil {
		return nil, err
	}
	g := map[string]int{}
	for _, d := range ds {
		g[d.Group]++
	}
	return g, nil
}

// UpdateDevice uses WATCH/MULTI optimistic locking so concurrent heartbeats and
// operator writes never clobber each other.
func (r *Redis) UpdateDevice(ctx context.Context, id string, fn func(*model.Device) error) (*model.Device, error) {
	var out *model.Device
	key := devKey(id)
	for attempt := 0; attempt < 10; attempt++ {
		err := r.c.Watch(ctx, func(tx *redis.Tx) error {
			b, err := tx.Get(ctx, key).Bytes()
			if errors.Is(err, redis.Nil) {
				return ErrNotFound
			}
			if err != nil {
				return err
			}
			d, err := clone[model.Device](b)
			if err != nil {
				return err
			}
			oldGroup := d.Group
			if err := fn(d); err != nil {
				return err
			}
			nb, err := json.Marshal(d)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
				p.Set(ctx, key, nb, 0)
				if oldGroup != d.Group {
					p.SRem(ctx, groupKey(oldGroup), id)
					p.SAdd(ctx, groupKey(d.Group), id)
				}
				return nil
			})
			out = d
			return err
		}, key)
		if errors.Is(err, redis.TxFailedErr) {
			continue
		}
		return out, err
	}
	return nil, redis.TxFailedErr
}

func (r *Redis) PushCommand(ctx context.Context, c model.Command) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return r.c.RPush(ctx, cmdKey(c.DeviceID), b).Err()
}

func (r *Redis) PopCommands(ctx context.Context, deviceID string, max int) ([]model.Command, error) {
	vals, err := r.c.LPopCount(ctx, cmdKey(deviceID), max).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]model.Command, 0, len(vals))
	for _, v := range vals {
		var c model.Command
		if err := json.Unmarshal([]byte(v), &c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

func (r *Redis) AddDetections(ctx context.Context, counts map[string]int64) error {
	_, err := r.c.Pipelined(ctx, func(p redis.Pipeliner) error {
		for id, n := range counts {
			p.IncrBy(ctx, detKey(id), n)
		}
		return nil
	})
	return err
}

func (r *Redis) PutRollout(ctx context.Context, ro *model.Rollout) error {
	b, err := json.Marshal(ro)
	if err != nil {
		return err
	}
	_, err = r.c.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, rolloutKey(ro.ID), b, 0)
		p.SAdd(ctx, "ef:rollouts", ro.ID)
		return nil
	})
	return err
}

func (r *Redis) GetRollout(ctx context.Context, id string) (*model.Rollout, error) {
	b, err := r.c.Get(ctx, rolloutKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return clone[model.Rollout](b)
}

func (r *Redis) ListRollouts(ctx context.Context) ([]*model.Rollout, error) {
	ids, err := r.c.SMembers(ctx, "ef:rollouts").Result()
	if err != nil {
		return nil, err
	}
	out := []*model.Rollout{}
	for _, id := range ids {
		ro, err := r.GetRollout(ctx, id)
		if err != nil {
			continue
		}
		out = append(out, ro)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}
