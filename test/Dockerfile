FROM golang:1.18-alpine

ENV CGO_ENABLED=0

ADD . /bin
WORKDIR /bin

RUN go test -c -o /bin/tests ./test/...

ENTRYPOINT [ "/bin/tests", "-test.timeout=1s", "-test.count=10", "-test.v" ]