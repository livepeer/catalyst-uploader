version ?= $(shell git describe --tag --dirty)

ldflags := -X 'main.Version=$(version)'
builddir := ../../build

.PHONY: all build run

all: build

build:
	go build -o $(builddir)/catalyst-uploader -ldflags="$(ldflags)" catalyst-uploader.go

run:
	go run -ldflags="$(ldflags)" catalyst-uploader.go $(args)
