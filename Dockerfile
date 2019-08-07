FROM golang:1.12.7 AS build
COPY . /go/src/github.com/bborbe/kafka-maxscale-cdc-connector
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-s" -a -installsuffix cgo -o /main ./src/github.com/bborbe/kafka-maxscale-cdc-connector
CMD ["/bin/bash"]

FROM alpine:3.9 as alpine
RUN apk --no-cache add ca-certificates

FROM scratch
COPY --from=build /main /main
COPY --from=alpine /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
ENTRYPOINT ["/main"]
