fmt:
	go fmt ./...

vet:
	go vet ./...

.PHONY: build
build: fmt vet
	go build -v -o bin/slatedb ./cmd

test: build
	go test -count=1 -cover -race ./...

test_coverage:
	go test -coverprofile=coverage.out ./...; \
	go tool cover -html="coverage.out"

lint:
	golangci-lint run

clean:
	go clean
	rm -rf bin/