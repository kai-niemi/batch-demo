#!/bin/bash

args=

jarfile=target/batch-demo.jar

if [ ! -f "$jarfile" ]; then
    ./mvnw clean install
fi

nohup java -jar batch-demo.jar $args > batch-demo-stdout.log 2>&1 &
