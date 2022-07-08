#!/bin/bash
set -e

# When current bench results were collected, the value was 6587.68
lscpu | grep BogoMIPS

go test -v . ./client
go test -v . ./client -race
go test -v ./bench/minikafka
pushd test && docker-compose up --abort-on-container-exit && popd
