FROM openjdk:8u212-jre-alpine

# Based on https://kafka.apache.org/quickstart
RUN apk add --no-cache bash ; \
    wget "https://dlcdn.apache.org/kafka/3.2.0/kafka_2.13-3.2.0.tgz" -O "/tmp/kafka_2.13-3.2.0.tgz" ; \
    mkdir -p /app ; \
    tar -xzf /tmp/kafka_2.13-3.2.0.tgz -C /app ; \
    mv /app/kafka_2.13-3.2.0 /app/kafka ; \
    chmod +x /app/kafka/bin/* ;

COPY ./*.sh /app/kafka/
RUN chmod +x /app/kafka/*.sh ;

WORKDIR /app/kafka

CMD ["./start-kafka.sh"]
