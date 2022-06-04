Boot kafka up:
```
docker-compose build
docker-compose up
```

Manual wiring of containers:
```
docker build . -t kafka
docker network create kafka_default

docker run \
    -p 2181:2181 \
    --net=kafka_default \
    --name zookeeper \ 
    wurstmeister/zookeeper

docker run \
    -it \
    --env KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
    --net=kafka_default \
    --name kafka \
    kafka \
    /bin/bash
```

From kafka container verify zookeeper reachability:
```
echo stat | nc zookeeper 2181
```