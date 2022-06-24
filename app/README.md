# RUN

You need to run this from parent directory to pick up local changes to core minikafka lib
```
docker build -f app/Dockerfile -t minikafka . && docker run -it --net host minikafka
```