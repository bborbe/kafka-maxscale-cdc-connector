
deps:
	go get -u golang.org/x/lint/golint
	go get -u github.com/kisielk/errcheck
	go get -u github.com/maxbrunsfeld/counterfeiter
	go get -u github.com/onsi/ginkgo/ginkgo
	go get -u golang.org/x/tools/cmd/goimports

precommit: ensure format generate test check addlicense
	@echo "ready to commit"

ensure:
	go mod verify
	go mod vendor

addlicense:
	@go get github.com/google/addlicense
	@addlicense -c "Benjamin Borbe" -y 2019 -l bsd ./*.go ./cdc/*.go ./cmd/*/*.go

generate:
	go get github.com/maxbrunsfeld/counterfeiter/v6
	rm -rf mocks
	go generate ./...

test:
	go test -cover -race $(shell go list ./... | grep -v /vendor/)

check: format lint vet errcheck

format:
	@go get golang.org/x/tools/cmd/goimports
	@find . -type f -name '*.go' -not -path './vendor/*' -exec gofmt -w "{}" +
	@find . -type f -name '*.go' -not -path './vendor/*' -exec goimports -w "{}" +

vet:
	@go vet $(shell go list ./... | grep -v /vendor/)

lint:
	@go get golang.org/x/lint/golint
	@golint -min_confidence 1 $(shell go list ./... | grep -v /vendor/)

errcheck:
	@go get github.com/kisielk/errcheck
	@errcheck -ignore '(Close|Write|Fprint)' $(shell go list ./... | grep -v /vendor/)
