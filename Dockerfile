FROM alpine:3.20.0
RUN apk --no-cache add ca-certificates
COPY bin/skbn /usr/local/bin/skbn
RUN addgroup -g 1001 -S skbn \
    && adduser -u 1001 -D -S -G skbn skbn
USER skbn
WORKDIR /home/skbn
CMD ["skbn"]
