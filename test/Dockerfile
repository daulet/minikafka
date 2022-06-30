FROM golang:1.18-alpine

ENV CGO_ENABLED=0

ADD . /bin
WORKDIR /bin

RUN go test -c -o /bin/tests ./test/...

ENTRYPOINT [ "/bin/tests", "-test.timeout=2s", "-test.count=2", "-test.v" ]