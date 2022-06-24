FROM golang:1.18-alpine

ENV CGO_ENABLED=0

ADD . /test
WORKDIR /test

RUN go test -c -o /test/minikafka test/...

ENTRYPOINT [ "/test/minikafka", "-test.timeout=1s" ]
