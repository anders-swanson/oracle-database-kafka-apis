# Oracle Database Kafka APIs

### Running the Oracle Database Kafka API tests

Prerequisites:
- Java 21
- Maven
- Docker

Once your docker environment is configured, you can run the integration tests with maven:


1. Run OKafkaExampleIT to demonstrate sending and receiving messages from a topic
```shell
mvn integration-test -Dit.test=OKafkaExampleIT
```

2. Run TransactionalProduceIT to demonstrate record transactions in an error state
```shell
mvn integration-test -Dit.test=TransactionalProduceIT
```