#!/bin/bash
set -e

go test -v . ./client
go test -v . ./client -race
go test -v ./bench/minikafka
pushd test && docker-compose up --abort-on-container-exit && popd
