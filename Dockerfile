FROM golang:1.10.3-alpine as builder
WORKDIR /go/src/github.com/maorfr/skbn/
COPY . .
RUN apk --no-cache add git glide \
    && glide up \
    && CGO_ENABLED=0 GOOS=linux go build -o skbn cmd/skbn.go

FROM alpine:3.8
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /go/src/github.com/maorfr/skbn/skbn /usr/local/bin/skbn
CMD ["skbn"]
