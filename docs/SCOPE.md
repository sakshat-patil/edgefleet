# EdgeFleet — Project Scope

## Problem
Edge camera fleets are hard to operate safely. A bad firmware push can brick hundreds of devices at once, config drift goes unnoticed, and there is no single view of which devices are healthy. EdgeFleet is a small but production-shaped control plane that addresses these three problems for a fleet of mixed ESP32 and Raspberry Pi cameras.

## Target numbers
| Variable | Value | Rationale |
|---|---|---|
| **N**: simulated edge cameras | **500** (plus 1–2 physical ESP32-CAMs and 2 Raspberry Pi 4s) | Enough to make staged rollouts meaningful (a 5% canary is 25 devices) and to load the control plane at ~250 heartbeats/sec, while still running on 5 small simulator pods. |
| **X**: sustained detection events/sec | **10,000 events/sec** | 500 cameras × ~20 detections/sec at a busy peak. Comfortable for a 3-broker Kafka cluster on m6i.large nodes with 12 partitions and RF=3. Stretch goal: 25k. |
| Heartbeat interval | 5 s (2 s in load tests) | |
| Rollout stages | 5% → 25% → 100%, 10% max failure rate | |
| Control plane p99 heartbeat latency | < 50 ms at N=500 | |

## In scope
1. **Go control plane**: REST API for registering and grouping devices, pushing config to a device or group, staged firmware rollouts with pause/resume/rollback, and per-device and per-group health checks. Device state lives in Redis (WATCH/MULTI optimistic locking) and commands go through AWS SQS (with a DLQ). Structured JSON logging and Prometheus metrics.
2. **Fleet simulator**: N virtual cameras with realistic telemetry and OTA timing, plus injectable firmware failures (random rate and a known-bad version).
3. **ESP32-CAM firmware (C, ESP-IDF)**: WiFi, heartbeat and command handling, NVS-persisted config, OTA into the inactive A/B slot with bootloader rollback, frame-difference motion detection, and detection events sent through the Kafka REST proxy. Tasks talk only through FreeRTOS queues.
4. **Raspberry Pi CV agent (Python, OpenCV)**: MOG2 background subtraction plus a HOG person detector run only on motion regions. Capture, detect, publish and heartbeat threads communicate through queues. Kafka producer with lz4 and idempotence. A/B updater for agent "firmware".
5. **Linux networking on the Pi**: 802.1Q VLANs (10 mgmt, 20 cameras, 30 uplink) persisted with systemd-networkd, and iptables default-deny where the camera VLAN can never reach the internet.
6. **Infrastructure**: Terraform for VPC, EKS (managed node group), ElastiCache Redis (multi-AZ), SQS + DLQ + alarm, ECR, firmware S3 bucket, and IRSA. Kubernetes manifests for the control plane (HPA, PDB, zone spread), Strimzi Kafka (KRaft) and load-test jobs.
7. **Load test**: `loadgen` job on EKS that measures sustained events/sec and produce latency.

## Out of scope
- A web UI (the API plus curl or Postman is the operator interface)
- Real ML models (YOLO etc.) on the Pi; HOG is enough to demonstrate the pipeline
- Multi-region, mTLS device identity and certificate provisioning (bearer tokens are used instead)
- Video storage or streaming; only detection metadata is sent

## Milestones
| # | Milestone | Done when | Status |
|---|---|---|---|
| 1 | Control plane core | Device CRUD, groups, config push, heartbeat; unit tests green | ✅ |
| 2 | Rollout engine | Staged promotion, soak, auto-rollback; tests cover success/failure/pause | ✅ |
| 3 | Simulator | 500 devices complete a rollout end to end locally | ✅ |
| 4 | Edge code | cv-agent tests green; firmware motion tests green; VLAN/iptables scripts | ✅ (firmware `idf.py build` runs in CI) |
| 5 | Infra | `terraform validate` green; `terraform apply` + `kubectl apply -k` on a real account | validate ✅ / apply ⏳ |
| 6 | Load test on EKS | `loadgen` sustains 10k events/sec for 10 min with 0 failed sends; numbers recorded in README | ⏳ |
| 7 | Hardware bring-up | 1 ESP32-CAM + 1 Pi on VLAN 20 receiving a rollout alongside simulated devices | ⏳ |

## Success metrics
- Bad firmware is stopped at the canary stage, with ≤ 10% of the group ever receiving it
- Config pushed to a group of 125 devices is applied by all of them within 2 heartbeat intervals
- ≥ 10,000 detection events/sec sustained on EKS with p99 produce latency < 100 ms
