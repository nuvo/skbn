FROM alpine:3.8

RUN apk add --no-cache ca-certificates

RUN addgroup -g 1001 -S skbn \
    && adduser -u 1001 -D -S -G skbn skbn 

COPY skbn /usr/local/bin/skbn
