#!/bin/bash -e

if [[ -z "$KAFKA_ADVERTISED_PORT" ]]; then
    export KAFKA_ADVERTISED_PORT=9092
fi

if [[ -z "$KAFKA_BROKER_ID" ]]; then
    export KAFKA_BROKER_ID=-1
fi

if [[ -z "$KAFKA_LOG_DIRS" ]]; then
    if [[ $KAFKA_BROKER_ID != -1 ]]; then
        export KAFKA_LOG_DIRS="/kafka/kafka-logs-$KAFKA_BROKER_ID"
    else
        export KAFKA_LOG_DIRS="/kafka/kafka-logs-$HOSTNAME"
    fi
fi

if [[ -z "$KAFKA_PORT" ]]; then
    export KAFKA_PORT=9092
fi

./create-topics.sh &
unset KAFKA_CREATE_TOPICS

{
    IFS=$'\n'
    for VAR in $(env)
    do
        env_var=$(echo "$VAR" | cut -d= -f1)
        
        if [[ $env_var =~ ^KAFKA_ ]]; then
            echo "Processing $env_var=${!env_var}"
            kafka_name=$(echo "$env_var" | cut -d_ -f2- | tr '[:upper:]' '[:lower:]' | tr _ .)
            echo "$kafka_name=${!env_var}" >> "./config/server.properties" # KAFKA_ZOOKEEPER_CONNECT
        fi
    done
}

exec "./bin/kafka-server-start.sh" "./config/server.properties"