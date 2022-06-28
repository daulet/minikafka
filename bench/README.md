## Benchmark minikafka
```
cd minikafka
go test -bench=. -run=^\$ -benchtime=30s -cpuprofile profile.out
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
Limit publisher connection to a single topic, avoid parsing of each message:
```
BenchmarkPublish1Topic-16             3412143             10551 ns/op
```
Stop serializing messages, send payload as plain bytes
```
BenchmarkPublish1Topic-16             4107202              8955 ns/op
```
Do not flush write buffer on every message
```
BenchmarkPublish1Topic-16             4573563              7746 ns/op
```
Adaptive number of publishers instead of fixed pool of workers
```
BenchmarkPublish1Topic-16             8201721              4268 ns/op
```