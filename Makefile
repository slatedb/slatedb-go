
fmt:
	go fmt ./...

vet:
	go vet ./...

.PHONY: gen_flatbuf
gen_flatbuf:
	flatc -o gen --go --gen-object-api --gen-all --gen-onefile --go-namespace flatbuf schemas/manifest.fbs
	go fmt ./gen/*.go

.PHONY: build
build: gen_flatbuf fmt vet
	go build -v -o bin/slatedb ./cmd

.PHONY: test
test: build
	go test -v -count=1 -cover -race ./...

test_coverage:
	go test -coverprofile=coverage.out ./...; \
	go tool cover -html="coverage.out"

lint:
	golangci-lint run

clean:
	go clean
	rm -rf bin/