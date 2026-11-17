import numpy as np

from edgefleet_agent.agent import Agent, make_event
from edgefleet_agent.config import Config
from edgefleet_agent.detector import Detector


def background(h=480, w=640):
    rng = np.random.default_rng(0)
    return (np.full((h, w, 3), 90, np.uint8) + rng.integers(0, 4, (h, w, 3), dtype=np.uint8))


def test_static_scene_has_no_detections():
    d = Detector(detect_people=False)
    for _ in range(30):
        out = d.process(background())
    assert out == []


def test_moving_object_is_detected_with_bbox():
    d = Detector(detect_people=False)
    for _ in range(30):
        d.process(background())
    frame = background()
    frame[200:300, 300:360] = 250  # 60x100 bright object
    out = d.process(frame)
    assert len(out) == 1
    x, y, w, h = out[0].bbox
    assert abs(x - 300) <= 12 and abs(y - 200) <= 12
    assert 50 <= w <= 80 and 90 <= h <= 120
    assert out[0].label == "motion"


def test_small_noise_below_min_area_ignored():
    d = Detector(detect_people=False, min_area=800)
    for _ in range(30):
        d.process(background())
    frame = background()
    frame[10:15, 10:15] = 255
    assert d.process(frame) == []


def test_large_frames_are_downscaled_but_bbox_in_original_pixels():
    d = Detector(detect_people=False)
    for _ in range(30):
        d.process(background(1080, 1920))
    frame = background(1080, 1920)
    frame[400:700, 900:1100] = 250
    (det,) = d.process(frame)
    x, y, w, h = det.bbox
    assert abs(x - 900) <= 30 and abs(y - 400) <= 30


def test_config_apply_clamps_and_ignores_unknown():
    c = Config()
    applied = c.apply({"fps": "99", "min_area": "1200", "detect_people": "false", "bogus": "1", "config_version": "7"})
    assert set(applied) == {"fps", "min_area", "detect_people", "config_version"}
    assert c.fps == 30.0 and c.min_area == 1200 and c.detect_people is False and c.config_version == 7


def test_set_config_command_is_deduplicated():
    a = Agent(Config())
    cmd = {"id": "cmd_1", "type": "set_config", "payload": {"min_area": "500", "config_version": "3"}}
    a.handle_command(cmd)
    a.cfg.min_area = 999
    a.handle_command(cmd)  # redelivery must not re-apply
    assert a.cfg.min_area == 999 and a.cfg.config_version == 3


def test_event_schema_matches_control_plane():
    from edgefleet_agent.detector import Detection

    ev = make_event("pi-01", 5, Detection("person", 0.8, (1, 2, 3, 4)))
    assert set(ev) == {"device_id", "seq", "label", "confidence", "bbox", "frame_ts"}
