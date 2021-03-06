## Benchmark minikafka
```
cd minikafka
go test -bench=. -benchmem -run=^\$ -benchtime=30s -cpuprofile profile.out
go tool pprof profile.out
(pprof) web
(pprof) list handle
```

To normalize across hardware differences, here is a snapshot:
```
$ lscpu | grep BogoMIPS 
BogoMIPS:                        6587.68
$ go test -bench=. -benchmem -run=^\$ -count=3
goos: linux
goarch: amd64
pkg: bench/minikafka
cpu: AMD Ryzen 9 5900HS with Radeon Graphics        
BenchmarkPublish1Topic-16         136630              8107 ns/op             368 B/op         10 allocs/op
BenchmarkPublish1Topic-16         148341              8074 ns/op             332 B/op         10 allocs/op
BenchmarkPublish1Topic-16         147409              7624 ns/op             294 B/op         10 allocs/op
BenchmarkThroughput-16            113343              9284 ns/op             382 B/op         15 allocs/op
BenchmarkThroughput-16            144178              8772 ns/op             392 B/op         15 allocs/op
BenchmarkThroughput-16            151826              8493 ns/op             395 B/op         15 allocs/op
BenchmarkEndToEndLatency-16       129237              8289 ns/op         868649066 ns(p99)           367 B/op         14 allocs/op
BenchmarkEndToEndLatency-16       128178              8405 ns/op         853505266 ns(p99)           363 B/op         14 allocs/op
BenchmarkEndToEndLatency-16       130908              8854 ns/op         940777821 ns(p99)           376 B/op         14 allocs/op
PASS
ok      bench/minikafka 35.191s
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
Use multiple publishers in the benchmark:
```
BenchmarkPublish1Topic-16        6590246              1893 ns/op             160 B/op          9 allocs/op
BenchmarkPublish1Topic-16        6302650              1895 ns/op             160 B/op          9 allocs/op
BenchmarkPublish1Topic-16        6272566              1950 ns/op             160 B/op          9 allocs/op
```

## End to End pub & sub
Initial:
```
BenchmarkThroughput-16           3000129             12708 ns/op           33735 B/op         22 allocs/op
BenchmarkThroughput-16           2851030             12028 ns/op           32641 B/op         21 allocs/op
BenchmarkThroughput-16           2943692             12047 ns/op           32306 B/op         21 allocs/op
```
Zero copy write from log file to subscriber's connection
```
BenchmarkThroughput-16           4554684              8198 ns/op            1041 B/op         27 allocs/op
BenchmarkThroughput-16           4182267              9025 ns/op             750 B/op         26 allocs/op
BenchmarkThroughput-16           3827070              9128 ns/op             748 B/op         26 allocs/op
```
[Don't stay on CPU when there are no changes to publish](https://github.com/daulet/minikafka/commit/9b1eb49def45d2df838635eabe29a588cca391d7#diff-879d7ff817ef6271a190861e67ab0ff3a2751136cedff764b3f680565540dc2bR390)
```
BenchmarkThroughput-16           4222543              8485 ns/op             702 B/op         16 allocs/op
BenchmarkThroughput-16           4476354              8253 ns/op             431 B/op         15 allocs/op
BenchmarkThroughput-16           4447429              8302 ns/op             410 B/op         15 allocs/op
```