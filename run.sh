#!/bin/bash

args=

jarfile=target/batch-demo.jar

if [ ! -f "$jarfile" ]; then
    ./mvnw clean install
fi

java -jar $jarfile $args $*