FROM golang:1.18-alpine as backend

ENV CGO_ENABLED=0

ADD . /build
WORKDIR /build

RUN \
    go build -o /build/minikafka

FROM scratch

COPY --from=backend /build/minikafka /srv/minikafka

ADD . /data
WORKDIR /srv
ENTRYPOINT ["/srv/minikafka"]
