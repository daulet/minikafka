FROM openjdk:8u212-jre-alpine

ARG kafka_version=2.8.1

ENV KAFKA_VERSION=$kafka_version

# Based on https://kafka.apache.org/quickstart
RUN apk add --no-cache bash ; \
    wget "https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz" -O "/tmp/kafka_2.13-${KAFKA_VERSION}.tgz" ; \
    mkdir -p /app ; \
    tar -xzf /tmp/kafka_2.13-${KAFKA_VERSION}.tgz -C /app ; \
    rm /tmp/kafka_2.13-${KAFKA_VERSION}.tgz ; \
    mv /app/kafka_2.13-${KAFKA_VERSION} /app/kafka ; \
    chmod +x /app/kafka/bin/* ;

COPY ./*.sh /app/kafka/
RUN chmod +x /app/kafka/*.sh ;

WORKDIR /app/kafka

CMD ["./start-kafka.sh"]
