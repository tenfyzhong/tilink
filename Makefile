GO ?= go
VERSION ?= dev

.PHONY: build worker test vet clean

build: worker
	mkdir -p bin
	$(GO) build -trimpath -ldflags "-s -w" -o bin/tilink ./cmd/tilink

worker:
	mkdir -p .build/worker internal/bundle/assets
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO) build -trimpath -tags lambda.norpc -ldflags "-s -w" -o .build/worker/bootstrap ./cmd/tilink-worker
	chmod 755 .build/worker/bootstrap
	cd .build/worker && zip -q -X ../../internal/bundle/assets/worker.zip bootstrap

test:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

clean:
	rm -rf bin .build internal/bundle/assets/worker.zip
