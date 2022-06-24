FROM golang:1.18-alpine

ENV CGO_ENABLED=0

ADD . /bin
WORKDIR /bin

# create separate layer to avoid redownloading the same dependencies
COPY go.* ./
RUN go mod download
COPY . ./

RUN go test -c -o /bin/tests test/...

ENTRYPOINT [ "/bin/tests", "-test.timeout=1s", "-test.v" ]
