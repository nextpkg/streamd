.PHONY: test race test-linux fixtures bench check e2e e2e-reload e2e-republish e2e-resume e2e-cloud e2e-cloud-cos e2e-cloud-container e2e-storage-fault e2e-upload-fault e2e-filesystem-fault e2e-redis

VERSION ?= development
IMAGE ?= streamd:$(VERSION)

.PHONY: build image e2e-release e2e-resources e2e-round-expiry e2e-completion-outcome
build:
	CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags="-s -w -X main.version=$(VERSION)" -o bin/streamd ./cmd/streamd

image:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE) .

e2e-release:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario release

.PHONY: e2e-stability
e2e-stability:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario stability

e2e-resources:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario resources

e2e-round-expiry:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario round-expiry

e2e-completion-outcome:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario completion-outcome

.PHONY: e2e-rotation
e2e-rotation:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario rotation

.PHONY: e2e-cloud-crash
e2e-cloud-crash:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario cloud-crash

.PHONY: e2e-joint-lifecycle
e2e-joint-lifecycle:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario joint-lifecycle

test:
	CGO_ENABLED=0 go test -count=1 -timeout=90s ./...

race:
	CGO_ENABLED=1 go test -race -count=1 -timeout=120s ./...

test-linux:
	./scripts/test-linux.sh

fixtures:
	./scripts/generate-media.sh

bench:
	CGO_ENABLED=0 go test -run '^$$' -bench . -benchmem -count=3 ./rtmp/core

check:
	go vet ./...

e2e:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario stream

.PHONY: e2e-fragment-clock
e2e-fragment-clock:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario fragment-clock

.PHONY: e2e-admin
e2e-admin:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario admin

e2e-reload:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario reload

.PHONY: e2e-recording-targets
e2e-recording-targets:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario recording-targets

.PHONY: e2e-recording-credentials
e2e-recording-credentials:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario recording-credentials

e2e-republish:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario republish

e2e-resume:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario resume

.PHONY: e2e-recording-crash
e2e-recording-crash:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario recording-crash

.PHONY: e2e-inspection-retry
e2e-inspection-retry:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario storage-inspection-retry

e2e-cloud:
	CGO_ENABLED=0 STREAMD_E2E_OBJECT_PROVIDER=aws go run ./scripts/e2e -scenario cloud-resume

e2e-cloud-cos:
	CGO_ENABLED=0 STREAMD_E2E_OBJECT_PROVIDER=cos go run ./scripts/e2e -scenario cloud-resume

e2e-cloud-container:
	CGO_ENABLED=0 STREAMD_E2E_OBJECT_PROVIDER=aws go run ./scripts/e2e -scenario cloud-container

e2e-storage-fault:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario storage-fault

e2e-upload-fault:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario upload-fault

e2e-filesystem-fault:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario filesystem-fault

e2e-redis:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario redis

.PHONY: e2e-redis-prepare
e2e-redis-prepare:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario redis-prepare

.PHONY: e2e-relay
e2e-relay:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario relay

.PHONY: e2e-origin
e2e-origin:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario origin

.PHONY: e2e-hls-origin
e2e-hls-origin:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario hls-origin

.PHONY: e2e-origin-fault
e2e-origin-fault:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario origin-fault

.PHONY: e2e-storage-inspection
e2e-storage-inspection:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario storage-inspection

.PHONY: e2e-storage-lifecycle
e2e-storage-lifecycle:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario storage-lifecycle

.PHONY: loadtest e2e-load-tools
loadtest:
	CGO_ENABLED=0 go build -o bin/streamd-loadtest ./scripts/loadtest

e2e-load-tools:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario load-tools

.PHONY: e2e-slow-consumer
e2e-slow-consumer:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario slow-consumer

.PHONY: e2e-slow-hls
e2e-slow-hls:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario slow-hls

.PHONY: e2e-rtmp-lifecycle
e2e-rtmp-lifecycle:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario rtmp-lifecycle

.PHONY: e2e-proxy
e2e-proxy:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario proxy

.PHONY: e2e-callbacks
e2e-callbacks:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario callbacks

.PHONY: e2e-callback-takeover e2e-callback-stress
.PHONY: e2e-metrics
e2e-metrics:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario metrics

e2e-callback-takeover:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario callback-takeover

e2e-callback-stress:
	CGO_ENABLED=0 go run ./scripts/e2e -scenario callback-stress
