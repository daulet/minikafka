FROM golang:1.18-alpine

ENV CGO_ENABLED=0

ADD . /test
WORKDIR /test

# create separate layer to avoid redownloading the same dependencies
COPY go.* ./
RUN go mod download
COPY . ./

RUN go test -c -o /test/minikafka test/...

ENTRYPOINT [ "/test/minikafka", "-test.timeout=1s" ]
