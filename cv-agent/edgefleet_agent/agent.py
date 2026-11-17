"""Agent main loop.

Three threads communicate only through queues (message passing):

  capture  -> frames_q  -> detect  -> events_q -> publish (Kafka)
  heartbeat: POSTs telemetry, receives commands, pushes them to commands_q,
             which the detect thread drains between frames.

Bounded queues drop the oldest frame rather than building latency.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import signal
import subprocess
import threading
import time
from datetime import datetime, timezone

import cv2
import requests

from . import __version__
from .config import Config
from .detector import Detector

log = logging.getLogger("edgefleet.agent")


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {"ts": datetime.now(timezone.utc).isoformat(), "level": record.levelname.lower(),
                 "logger": record.name, "msg": record.getMessage()}
        entry.update(getattr(record, "fields", {}))
        return json.dumps(entry)


def read_cpu_temp() -> float:
    try:
        with open("/sys/class/thermal/thermal_zone0/temp") as f:
            return int(f.read()) / 1000.0
    except OSError:
        return 0.0


def read_mem_free_kb() -> int:
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1])
    except OSError:
        pass
    return 0


def read_cpu_percent(prev: list[int]) -> float:
    try:
        with open("/proc/stat") as f:
            fields = [int(v) for v in f.readline().split()[1:]]
    except OSError:
        return 0.0
    idle, total = fields[3] + fields[4], sum(fields)
    d_idle, d_total = idle - prev[0], total - prev[1]
    prev[:] = [idle, total]
    return 100.0 * (1 - d_idle / d_total) if d_total else 0.0


def make_event(device_id: str, seq: int, det) -> dict:
    return {"device_id": device_id, "seq": seq, "label": det.label, "confidence": det.confidence,
            "bbox": list(det.bbox), "frame_ts": datetime.now(timezone.utc).isoformat()}


class Agent:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.stop = threading.Event()
        self.frames_q: queue.Queue = queue.Queue(maxsize=2)
        self.events_q: queue.Queue = queue.Queue(maxsize=1000)
        self.commands_q: queue.Queue = queue.Queue()
        self.errors = 0
        self.measured_fps = 0.0
        self.firmware_version = os.environ.get("EF_FIRMWARE_VERSION", __version__)
        self.seen: set[str] = set()
        self.session = requests.Session()
        if cfg.device_token:
            self.session.headers["Authorization"] = f"Bearer {cfg.device_token}"

    # ---------------------------------------------------------- threads
    def capture_loop(self) -> None:
        src = int(self.cfg.source) if self.cfg.source.isdigit() else self.cfg.source
        cap = cv2.VideoCapture(src)
        while not self.stop.is_set():
            ok, frame = cap.read()
            if not ok:
                self.errors += 1
                time.sleep(1)
                cap.release()
                cap = cv2.VideoCapture(src)
                continue
            try:
                self.frames_q.put_nowait(frame)
            except queue.Full:  # drop oldest so latency stays bounded
                try:
                    self.frames_q.get_nowait()
                except queue.Empty:
                    pass
                self.frames_q.put_nowait(frame)
            time.sleep(1.0 / self.cfg.fps)
        cap.release()

    def detect_loop(self) -> None:
        det = Detector(self.cfg.motion_threshold, self.cfg.min_area, self.cfg.detect_people)
        seq, frames, window_start = 0, 0, time.monotonic()
        while not self.stop.is_set():
            self._drain_commands(det)
            try:
                frame = self.frames_q.get(timeout=1)
            except queue.Empty:
                continue
            for d in det.process(frame):
                seq += 1
                try:
                    self.events_q.put_nowait(make_event(self.cfg.device_id, seq, d))
                except queue.Full:
                    self.errors += 1
            frames += 1
            if (now := time.monotonic()) - window_start >= 5:
                self.measured_fps = frames / (now - window_start)
                frames, window_start = 0, now

    def publish_loop(self) -> None:
        from confluent_kafka import Producer  # imported lazily so tests need no librdkafka

        producer = Producer({"bootstrap.servers": self.cfg.kafka_brokers, "linger.ms": 50,
                             "compression.type": "lz4", "enable.idempotence": True,
                             "client.id": self.cfg.device_id})

        def on_delivery(err, _msg):
            if err is not None:
                self.errors += 1
                log.warning("kafka delivery failed", extra={"fields": {"err": str(err)}})

        while not self.stop.is_set():
            try:
                ev = self.events_q.get(timeout=0.5)
            except queue.Empty:
                producer.poll(0)
                continue
            producer.produce(self.cfg.kafka_topic, key=self.cfg.device_id, value=json.dumps(ev), on_delivery=on_delivery)
            producer.poll(0)
        producer.flush(5)

    def heartbeat_loop(self) -> None:
        base = self.cfg.control_plane_url.rstrip("/")
        self.session.post(f"{base}/v1/devices", timeout=5, json={
            "id": self.cfg.device_id, "kind": "rpi-cv", "group": self.cfg.group, "firmware_version": self.firmware_version})
        cpu_prev = [0, 0]
        read_cpu_percent(cpu_prev)
        boot = time.monotonic()
        while not self.stop.wait(self.cfg.heartbeat_sec):
            hb = {"firmware_version": self.firmware_version, "firmware_state": "ok",
                  "config_version": self.cfg.config_version,
                  "telemetry": {"cpu_percent": round(read_cpu_percent(cpu_prev), 1), "mem_free_kb": read_mem_free_kb(),
                                "temp_c": read_cpu_temp(), "fps": round(self.measured_fps, 1), "errors": self.errors,
                                "uptime_sec": int(time.monotonic() - boot)}}
            try:
                r = self.session.post(f"{base}/v1/agent/{self.cfg.device_id}/heartbeat", json=hb, timeout=5)
                r.raise_for_status()
                for cmd in r.json().get("commands", []):
                    self.commands_q.put(cmd)
            except requests.RequestException as e:
                self.errors += 1
                log.warning("heartbeat failed", extra={"fields": {"err": str(e)}})

    # ---------------------------------------------------------- commands
    def _drain_commands(self, det: Detector) -> None:
        while True:
            try:
                cmd = self.commands_q.get_nowait()
            except queue.Empty:
                return
            self.handle_command(cmd, det)

    def handle_command(self, cmd: dict, det: Detector | None = None) -> None:
        cid, ctype, payload = cmd.get("id"), cmd.get("type"), cmd.get("payload") or {}
        if not cid or cid in self.seen:
            return
        self.seen.add(cid)
        log.info("command received", extra={"fields": {"command_id": cid, "type": ctype}})
        if ctype == "set_config":
            applied = self.cfg.apply(payload)
            if det is not None:
                det.min_area = self.cfg.min_area
                det.detect_people = self.cfg.detect_people
                det.set_threshold(self.cfg.motion_threshold)
            log.info("config applied", extra={"fields": {"keys": applied, "config_version": self.cfg.config_version}})
        elif ctype in ("update_firmware", "rollback_firmware"):
            # On Pi nodes "firmware" is the agent package; the updater script
            # installs the requested version into an A/B venv and restarts the unit.
            version = payload.get("version", "")
            subprocess.Popen(["/usr/local/bin/edgefleet-update", version, payload.get("url", "")])
        elif ctype == "reboot":
            subprocess.Popen(["systemctl", "restart", "edgefleet-agent"])

    def run(self) -> None:
        threads = [threading.Thread(target=f, name=f.__name__, daemon=True)
                   for f in (self.capture_loop, self.detect_loop, self.publish_loop, self.heartbeat_loop)]
        for t in threads:
            t.start()
        log.info("agent started", extra={"fields": {"device_id": self.cfg.device_id, "version": self.firmware_version}})
        self.stop.wait()
        for t in threads:
            t.join(timeout=5)


def main() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), handlers=[handler])
    agent = Agent(Config())
    signal.signal(signal.SIGTERM, lambda *_: agent.stop.set())
    signal.signal(signal.SIGINT, lambda *_: agent.stop.set())
    agent.run()


if __name__ == "__main__":
    main()
