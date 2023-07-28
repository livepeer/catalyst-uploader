GO_BUILD_DIR?=build/

ldflags := -X 'main.Version=$(shell git rev-parse HEAD)'

.PHONY: all
all: build fmt test tidy

.PHONY: build
build:
	go build -ldflags="$(ldflags)" -o "$(GO_BUILD_DIR)catalyst-uploader" .

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: test
test:
	go test -race ./...

.PHONY: tidy
tidy:
	go mod tidy
