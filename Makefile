LINT = $(GOPATH)/bin/golangci-lint
LINT_VERSION = v1.61.0

$(LINT): ## Download Go linter
        curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin $(LINT_VERSION)

.PHONY: lint
lint: $(LINT) ## Run Go linter
	$(LINT) run -v ./...

fmt:
	go fmt ./...

vet:
	go vet ./...

.PHONY: flatbuf
flatbuf:
	flatc -o internal/flatbuf --go --gen-object-api --gen-all --gen-onefile --go-namespace flatbuf internal/flatbuf/schemas/manifest.fbs
	go fmt ./internal/flatbuf/*.go

.PHONY: build
build: flatbuf fmt vet
	go build -v -o bin/slatedb -race ./cmd

test_coverage:
	go test -coverprofile=coverage.out ./...; \
	go tool cover -html="coverage.out"

clean:
	go clean
	rm -rf bin/

.PHONY: test
test:
	go test -timeout 10m -v -p=1 -count=1 -race ./...

.PHONY: tidy
tidy:
	go mod tidy && git diff --exit-code

.PHONY: ci
ci: tidy lint test
	@echo
	@echo "\033[32mEVERYTHING PASSED!\033[0m"