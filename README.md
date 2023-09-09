# Kafka Streams Playground

![Playground build](https://github.com/PPrydorozhnyi/kafka-streams-playground/actions/workflows/gradle.yml/badge.svg)

Number of small projects to play around Apache Kafka Streams with Spring Boot

## Project Structure

- word-count - basic application to count words using Kafka Streams.
- favourite-colour - application to determine how many users like certain colour. also Kafka streams with 
  intermediate table.
- balance - application which contains aggregation with exactly one semantics and additional join stream-table with 
  favourite colour for the same user. the subproject contains topology and integration tests.

## Get the app

____
Prerequisites

- Java 17

Before you can run the application, you need to get the application source code onto your machine.

1. Clone the getting-started repository using the following command:

```bash
git clone git@github.com:PPrydorozhnyi/kafka-streams-playground.git
```

2Build and run project locally inside the certain module:

```bash
./gradlew bootRun
```

## Tests

Topology and integration tests inside <i>balance-app</i> module

## Dependencies
- Kafka

## Maintainers

____
petro.prydorozhnyi@gmail.com
