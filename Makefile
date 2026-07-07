VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS = -s -w \
	-X main.version=$(VERSION) \
	-X main.commit=$(COMMIT) \
	-X main.date=$(DATE)

.PHONY: build test lint run clean

build:
	go build -ldflags "$(LDFLAGS)" -o bin/piko ./cmd/piko

test:
	go test -race ./...

lint:
	golangci-lint run

run: build
	./bin/piko -config config.yaml

clean:
	rm -rf bin dist
