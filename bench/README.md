## Benchmark minikafka
```
cd minikafka
go test -bench=. -benchmem -run=^\$ -benchtime=30s -cpuprofile profile.out
go tool pprof profile.out
(pprof) web
(pprof) list handle
```

## Publish & ack
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
Fix a bug in previous "improvement" - we were not publishing all messages
```
BenchmarkPublish1Topic-16        4433725              7851 ns/op             596 B/op         10 allocs/op
BenchmarkPublish1Topic-16        4621489              7754 ns/op             314 B/op         10 allocs/op
BenchmarkPublish1Topic-16        4418972              7768 ns/op             296 B/op         10 allocs/op
```

## End to End pub & sub
Initial:
```
BenchmarkThroughput-16           3000129             12708 ns/op           33735 B/op         22 allocs/op
BenchmarkThroughput-16           2851030             12028 ns/op           32641 B/op         21 allocs/op
BenchmarkThroughput-16           2943692             12047 ns/op           32306 B/op         21 allocs/op
```