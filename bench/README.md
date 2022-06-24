## Benchmark minikafka
```
cd minikafka
go test -bench=. -benchtime=30s -cpuprofile profile.out
go tool pprof profile.out
(pprof) web
(pprof) list handle
```