# Run
```
docker-compose build && docker-compose up --abort-on-container-exit
```

# Debug
docker-compose.yml:
```
version: '2'
services:
  minikafka:
    build:
      context: ..
      dockerfile: app/Dockerfile

  toxiproxy:
    image: ghcr.io/shopify/toxiproxy
```

Dockerfile:
```
FROM golang:1.18-alpine

ENV CGO_ENABLED=0

ADD . /src
WORKDIR /src

RUN go test -c -o /bin/tests ./test/...

ENTRYPOINT [ "/bin/sh" ]
```

Wire things up:
```
docker-compose build && docker-compose up --abort-on-container-exit
docker build -t runner -f test/Dockerfile .
docker run -it --network test_default -v $(pwd)/test:/src/test runner
```
