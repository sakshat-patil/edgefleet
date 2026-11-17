"""Agent configuration: env vars at boot, then live updates from set_config."""

from __future__ import annotations

import dataclasses
import os
import threading


@dataclasses.dataclass
class Config:
    device_id: str = os.environ.get("EF_DEVICE_ID", os.uname().nodename)
    group: str = os.environ.get("EF_GROUP", "rpi")
    control_plane_url: str = os.environ.get("EF_CONTROL_PLANE_URL", "http://10.20.0.1:8080")
    device_token: str = os.environ.get("EF_DEVICE_TOKEN", "")
    kafka_brokers: str = os.environ.get("EF_KAFKA_BROKERS", "10.20.0.1:9092")
    kafka_topic: str = os.environ.get("EF_KAFKA_TOPIC", "detections")
    source: str = os.environ.get("EF_SOURCE", "0")  # camera index, RTSP URL or video file
    heartbeat_sec: float = float(os.environ.get("EF_HEARTBEAT_SEC", "5"))

    # Live-tunable via PATCH /v1/devices/{id}/config
    fps: float = 10.0
    min_area: int = 800  # px^2; contours smaller than this are noise
    motion_threshold: int = 25  # MOG2 varThreshold
    detect_people: bool = True  # run HOG person detector on motion regions
    config_version: int = 0

    _lock: threading.Lock = dataclasses.field(default_factory=threading.Lock, repr=False, compare=False)

    def apply(self, payload: dict[str, str]) -> list[str]:
        """Apply a set_config payload. Returns the keys that were applied."""
        casts = {"fps": float, "min_area": int, "motion_threshold": int, "config_version": int,
                 "detect_people": lambda v: v.lower() in ("1", "true", "yes")}
        applied = []
        with self._lock:
            for key, raw in payload.items():
                cast = casts.get(key)
                if cast is None:
                    continue
                try:
                    setattr(self, key, cast(raw))
                    applied.append(key)
                except ValueError:
                    pass
            self.fps = min(max(self.fps, 1.0), 30.0)
        return applied

    def snapshot(self) -> dict:
        with self._lock:
            return {f.name: getattr(self, f.name) for f in dataclasses.fields(self) if not f.name.startswith("_")}
