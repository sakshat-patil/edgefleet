.PHONY: test test-go test-py test-fw run sim loadtest up down fmt tf-validate

test: test-go test-py test-fw

test-go:
	go vet ./...
	go test -race ./...

test-py:
	cd cv-agent && python -m pytest -q

test-fw:
	cc -Wall -Wextra -Werror -Ifirmware/esp32-cam/main firmware/esp32-cam/main/motion.c firmware/esp32-cam/test/test_motion.c -o /tmp/ef_motion_test && /tmp/ef_motion_test

run:            ## control plane with in-memory store/queue
	go run ./cmd/control-plane

sim:            ## 500 simulated cameras against localhost
	go run ./cmd/simulator -n 500

up:
	docker compose up --build -d

down:
	docker compose down -v

loadtest:
	docker compose --profile loadtest run --rm loadgen -devices 500 -rate 10000 -duration 2m

fmt:
	gofmt -w . && tofu -chdir=infra/terraform fmt

tf-validate:
	cd infra/terraform && terraform init -backend=false && terraform validate
