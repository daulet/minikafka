#!/bin/bash -e

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
        
        echo "Processing env var: $env_var"

        if [[ $env_var =~ ^KAFKA_ ]]; then
            kafka_name=$(echo "$env_var" | cut -d_ -f2- | tr '[:upper:]' '[:lower:]' | tr _ .)
            echo "$kafka_name=${!env_var}" >> "./config/server.properties" # KAFKA_ZOOKEEPER_CONNECT
        fi
    done
}

exec "./bin/kafka-server-start.sh" "./config/server.properties"