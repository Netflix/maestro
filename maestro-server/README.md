# Maestro Server
===================================
This module includes a Springboot app implementation of Maestro server.

## Docker

### Run

Assuming you have cockroachdb already setup and running ([otherwise check these steps](#cockroachdb)):

```shell
docker run \
  -p 8080:8080 \
  -e CONDUCTOR_CONFIGS_JDBCURL=jdbc:postgresql://cockroachdb:26257/maestro \
  -e CONDUCTOR_CONFIGS_JDBCUSERNAME=root \
  datacatering/maestro:0.1.0
```

### Build

Build the JAR file and Docker image via:

```shell
./gradlew bootJar
docker build -t maestro-server:0.1 .
```

#### CockroachDB

Spin up cockroachdb either yourself or via [insta-infra](https://github.com/data-catering/insta-infra), with database 
`maestro` created.

```shell
./run.sh cockroach
./run.sh -c cockroach
CREATE DATABASE maestro;
```

```shell
docker run \
  -p 8080:8080 \
  -e CONDUCTOR_CONFIGS_JDBCURL=jdbc:postgresql://cockroachdb:26257/maestro \
  -e CONDUCTOR_CONFIGS_JDBCUSERNAME=root \
  maestro-server:0.1
```
