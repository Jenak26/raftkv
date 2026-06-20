# raftkv — developer task runner.
# On Windows, run these under Git Bash, WSL, or install `make` (e.g. choco install make).

GO   ?= go
PKGS := ./...

.PHONY: all build test race vet fmt fmtcheck lint tidy bench chaos cover clean

## all: format, vet, and run the race-enabled tests (the default gate)
all: fmtcheck vet race

## build: compile every package
build:
	$(GO) build $(PKGS)

## test: run all tests
test:
	$(GO) test -count=1 $(PKGS)

## race: run all tests under the race detector (the real correctness gate)
race:
	$(GO) test -race -count=1 $(PKGS)

## vet: run go vet
vet:
	$(GO) vet $(PKGS)

## fmt: format the tree in place
fmt:
	$(GO) fmt $(PKGS)

## fmtcheck: fail if any file is not gofmt-clean
fmtcheck:
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then echo "gofmt needed on:"; echo "$$unformatted"; exit 1; fi

## tidy: tidy and verify the module graph
tidy:
	$(GO) mod tidy

## lint: run golangci-lint if installed (optional)
lint:
	@command -v golangci-lint >/dev/null 2>&1 && golangci-lint run || \
		echo "golangci-lint not installed; skipping (see https://golangci-lint.run)"

## bench: run benchmarks (Phase 10+)
bench:
	$(GO) test -bench=. -benchmem -run='^$$' ./bench/...

## chaos: run the seeded fault-injection suite (Phase 9+). Replay with: make chaos SEED=0x...
chaos:
	$(GO) test -race -count=1 ./test/chaos/... $(if $(SEED),-args -seed=$(SEED),)

## cover: produce a coverage profile and summary
cover:
	$(GO) test -coverprofile=coverage.out $(PKGS)
	$(GO) tool cover -func=coverage.out | tail -n 1

## clean: remove build/test artifacts
clean:
	$(GO) clean
	rm -f coverage.out
