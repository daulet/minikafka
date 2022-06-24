## Benchmark minikafka
```
cd minikafka
go test -bench=. -benchtime=30s -cpuprofile profile.out
go tool pprof profile.out
(pprof) web
(pprof) list handle
```

## Results
```
goos:linux                                          
goarch: amd64                  
pkg: bench/minikafka
cpu: AMD Ryzen 9 5900HS with Radeon Graphics
BenchmarkPublish1Topic-16             1000000             35166 ns/op
PASS                          
ok      bench/minikafka 36.021s
```