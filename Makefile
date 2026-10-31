.PHONY: test race test-linux fixtures bench check

VERSION ?= development
IMAGE ?= streamd:$(VERSION)

.PHONY: build image
build:
	CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags="-s -w -X main.version=$(VERSION)" -o bin/streamd ./cmd/streamd

image:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE) .

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

.PHONY: loadtest
loadtest:
	CGO_ENABLED=0 go build -o bin/streamd-loadtest ./scripts/loadtest


.PHONY: e2e e2e-core e2e-list e2e-cloud e2e-cloud-cos e2e-inspection-retry
e2e: e2e-stream

e2e-core: fixtures
	CGO_ENABLED=0 go run ./scripts/e2e -suite core

e2e-list:
	CGO_ENABLED=0 go run ./scripts/e2e -list

e2e-%: fixtures
	CGO_ENABLED=0 go run ./scripts/e2e -scenario $*

e2e-cloud: fixtures
	CGO_ENABLED=0 STREAMD_E2E_OBJECT_PROVIDER=aws go run ./scripts/e2e -scenario cloud-resume

e2e-cloud-cos: fixtures
	CGO_ENABLED=0 STREAMD_E2E_OBJECT_PROVIDER=cos go run ./scripts/e2e -scenario cloud-resume

e2e-cloud-container: fixtures
	CGO_ENABLED=0 STREAMD_E2E_OBJECT_PROVIDER=aws go run ./scripts/e2e -scenario cloud-container

e2e-inspection-retry: fixtures
	CGO_ENABLED=0 go run ./scripts/e2e -scenario storage-inspection-retry


.PHONY: verify
verify: test check
	$(MAKE) e2e-core
