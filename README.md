# Oracle Database Kafka APIs

### Running the Oracle Database Kafka API tests

Prerequisites:
- Java 21
- Maven
- Docker

1. Start a local Oracle Database 23ai container.

```shell
docker run --name oracledb -d -p 1521:1521 -e ORACLE_PASSWORD=<MY PASSWORD> gvenzl/oracle-free:23.4-slim-faststart
```

2. Start a shell onto the container.

```shell
docker exec -it oracledb bash
```

3. Start SQLPlus as `sysdba`, using the ORACLE_PASSWORD value specified when the container was started to login.

```shell
sqlplus sys/<MY PASSWORD>@freepdb1 as sysdba
```

4. Run the queries in `src/test/resources/okafka.sql` to configure the database for the Kafka tests.

5. Quit sqlplus and exit the container.

Once the database is configured, and you've quit the database container shell, you may run the integration tests with maven:

```shell
mvn integration-test
```