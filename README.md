[![Java CI with Maven](https://github.com/kai-niemi/batch-demo/actions/workflows/maven.yml/badge.svg)](https://github.com/kai-niemi/batch-demo/actions/workflows/maven.yml)

<!-- TOC -->
* [Introduction](#introduction)
  * [Compatibility](#compatibility)
* [Building and Running](#building-and-running)
  * [Install the JDK](#install-the-jdk)
  * [Clone the project](#clone-the-project)
  * [Build the artifact](#build-the-artifact)
  * [Running](#running)
* [Terms of Use](#terms-of-use)
<!-- TOC -->

# Introduction

A simple batch insert load testing tool for CockroachDB. 

Features:

- Supports both ordinary JDBC large batch statements 
and `insert into .. select` using arrays. 
- Table names and column count/size can be customized.
- Either virtual threads (JDK21+) or platform threads
- Connection pooling via Hikari
- Datasource proxy SQL trace logging via TTDDYY

## Compatibility

- JDK21+
- MacOS (main platform)
- Linux
- CockroachDB

# Building and Running

## Install the JDK

MacOS (using sdkman):

    curl -s "https://get.sdkman.io" | bash
    sdk list java
    sdk install java 21.0 (pick version)  

Ubuntu:

    sudo apt-get install openjdk-21-jdk

## Clone the project

    git clone git@github.com:kai-niemi/batch-demo && cd batch-demo

## Build the artifact

    chmod +x mvnw
    ./mvnw clean install

## Running

To print usage help:

    java -jar target/batch-demo.jar <args>

Example running array batch insert's against a secure cluster for 2 hours:

    echo "--url jdbc:postgresql://localhost:26257/defaultdb?ssl=true&sslmode=require" > cmd.txt
    echo "--user roachprod" >> cmd.txt
    echo "--password cockroachdb" >> cmd.txt
    echo "--concurrency 4" >> cmd.txt
    echo "--duration 2h" >> cmd.txt
    echo "--batch-size 64" >> cmd.txt
    echo "array-insert" >> cmd.txt

    java -jar target/batch-demo.jar $( cat cmd.txt )

# Terms of Use

This tool is not supported by Cockroach Labs. Use of this tool is entirely at your
own risk and Cockroach Labs makes no guarantees or warranties about its operation.

See [MIT](LICENSE.txt) for terms and conditions.
