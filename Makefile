cmd ?= catalyst-uploader
version ?= $(shell git describe --tag --dirty)
tags ?= latest $(version)

allCmds := $(shell ls ./cmd/)

.PHONY: all $(allCmds)

all: $(allCmds)

$(allCmds):
	$(MAKE) -C ./cmd/$@

run:
	$(MAKE) -C ./cmd/$(cmd) run
