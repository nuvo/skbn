# HAS_DEP := $(shell command -v dep;)
# DEP_VERSION := v0.5.0
GIT_TAG := $(shell git tag | tail -n 1)
GIT_COMMIT := $(shell git rev-parse --short HEAD)
LDFLAGS := "-X main.GitTag=${GIT_TAG} -X main.GitCommit=${GIT_COMMIT}"
DIST := $(CURDIR)/dist
DOCKER_USER := $(shell printenv DOCKER_USER)
DOCKER_PASSWORD := $(shell printenv DOCKER_PASSWORD)
TRAVIS := $(shell printenv TRAVIS)

all: bootstrap build docker push

fmt:
	go fmt ./pkg/... ./cmd/...

vet:
	go vet ./pkg/... ./cmd/...

# Build skbn binary
build: fmt vet
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags $(LDFLAGS) -o bin/skbn cmd/skbn.go

# Build skbn docker image
docker: fmt vet
	cp bin/skbn skbn
	docker build -t nuvo/skbn:latest .
	rm skbn


# Push will only happen in travis ci
push:
ifdef TRAVIS
ifdef DOCKER_USER
ifdef DOCKER_PASSWORD
	docker login -u $(DOCKER_USER) -p $(DOCKER_PASSWORD)
	docker push nuvo/skbn:latest
endif
endif
endif

bootstrap:
# ifndef HAS_DEP
# 	wget -q -O $(GOPATH)/bin/dep https://github.com/golang/dep/releases/download/$(DEP_VERSION)/dep-linux-amd64
# 	chmod +x $(GOPATH)/bin/dep
# endif
#	dep ensure

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