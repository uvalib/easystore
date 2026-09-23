GOCMD = go
GOTEST = $(GOCMD) test
GOGET = $(GOCMD) get
GOMOD = $(GOCMD) mod
GOFMT = $(GOCMD) fmt
GOVET = $(GOCMD) vet
PACKAGENAME = uvaeasystore

# the direct datastore implementation is behind this tag and the test files reference it,
# so anything that compiles the package (including its tests) needs it
BUILDTAGS = -tags service

# where "go install" puts binaries: GOBIN when it is set, GOPATH/bin otherwise
TOOLBIN = $(shell $(GOCMD) env GOBIN)
ifeq ($(strip $(TOOLBIN)),)
TOOLBIN = $(shell $(GOCMD) env GOPATH)/bin
endif

STATICCHECKS = all,-ST1000,-S1002,-ST1003,-ST1020,-ST1021,-ST1022

build: test

test:
	cd $(PACKAGENAME); $(GOTEST) $(BUILDTAGS) -v $(if $(TEST),-run '$(TEST)',)

dep:
	cd $(PACKAGENAME); $(GOGET) -u
	cd $(PACKAGENAME); $(GOMOD) tidy

fmt:
	cd $(PACKAGENAME); $(GOFMT)

vet:
	cd $(PACKAGENAME); $(GOVET) $(BUILDTAGS)

check:
	$(GOCMD) install honnef.co/go/tools/cmd/staticcheck@latest
	cd $(PACKAGENAME); $(TOOLBIN)/staticcheck $(BUILDTAGS) -checks $(STATICCHECKS) *.go
	$(GOCMD) install golang.org/x/tools/go/analysis/passes/shadow/cmd/shadow@latest
	cd $(PACKAGENAME); $(GOVET) $(BUILDTAGS) -vettool=$(TOOLBIN)/shadow
