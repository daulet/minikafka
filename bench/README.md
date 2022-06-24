## Benchmark minikafka
```
cd minikafka
go test -bench=. -benchtime=30s -cpuprofile profile.out
go tool pprof profile.out
(pprof) web
(pprof) list handle
```

## Results
Initial result:
```
BenchmarkPublish1Topic-16             1000000             35166 ns/op
```
After reusing MessageReader's temporary buffer:
```
BenchmarkPublish1Topic-16             1210952             28874 ns/op
```