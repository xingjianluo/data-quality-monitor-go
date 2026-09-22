.PHONY: build test race cover vet fmt demo clean

build:
	go build ./...

test:
	go test ./... -count=1

race:
	go test ./... -race -count=1

cover:
	go test ./... -cover

vet:
	go vet ./...

fmt:
	gofmt -l -w .

# 端到端演示：b1 通过（退出码 0），b2 产生可区分的失败（退出码 3），重放 b1 幂等。
demo: build
	@tmp=$$(mktemp -d); \
	go run ./cmd/dqm run --plan testdata/workspace/plan-b1.json --state $$tmp/state.json -v || true; \
	go run ./cmd/dqm run --plan testdata/workspace/plan-b2.json --state $$tmp/state.json -v || true; \
	go run ./cmd/dqm run --plan testdata/workspace/plan-b1.json --state $$tmp/state.json -v || true; \
	go run ./cmd/dqm history --state $$tmp/state.json; \
	rm -rf $$tmp

clean:
	rm -f dqm-state.json
