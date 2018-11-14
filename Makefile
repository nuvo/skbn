HAS_DEP := $(shell command -v dep;)
DEP_VERSION := v0.5.0
GIT_TAG := $(shell git describe --tags --always)
GIT_COMMIT := $(shell git rev-parse --short HEAD)
LDFLAGS := "-X main.GitTag=${GIT_TAG} -X main.GitCommit=${GIT_COMMIT}"
DIST := $(CURDIR)/dist

all: bootstrap build

fmt:
	go fmt ./pkg/... ./cmd/...

vet:
	go vet ./pkg/... ./cmd/...

# Build skbn binary
build: fmt vet
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags $(LDFLAGS) -o bin/skbn cmd/skbn.go

bootstrap:
ifndef HAS_DEP
	wget -q -O $(GOPATH)/bin/dep https://github.com/golang/dep/releases/download/$(DEP_VERSION)/dep-linux-amd64
	chmod +x $(GOPATH)/bin/dep
endif
	dep ensure

dist:
	mkdir -p $(DIST)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags $(LDFLAGS) -o skbn cmd/skbn.go
	tar -zcvf $(DIST)/skbn-linux-$(GIT_TAG).tgz skbn
	rm skbn
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -ldflags $(LDFLAGS) -o skbn cmd/skbn.go
	tar -zcvf $(DIST)/skbn-macos-$(GIT_TAG).tgz skbn
	rm skbn
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -ldflags $(LDFLAGS) -o skbn.exe cmd/skbn.go
	tar -zcvf $(DIST)/skbn-windows-$(GIT_TAG).tgz skbn.exe
	rm skbn.exe