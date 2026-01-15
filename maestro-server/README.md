Maestro Server
===================================
This module includes a Springboot app implementation of Maestro server.

## Docker

### Build

Build the JAR file and Docker image via:

```shell
./gradlew bootJar
docker build -t maestro-server:0.1 maestro-server
```

### Run

Assuming you have CockroachDB already setup and running ([otherwise check these steps](#cockroachdb)):

```shell
docker run \
  --name maestro \
  --network host \
  -p 8080:8080 \
  -e CONDUCTOR_CONFIGS_JDBCURL=jdbc:postgresql://localhost:26257/maestro \
  -e CONDUCTOR_CONFIGS_JDBCUSERNAME=root \
  maestro-server:0.1
```

#### CockroachDB

Spin up CockroachDB with `maestro` database via the below commands:

```shell
docker run -d -p 26257:26257 --name cockroachdb cockroachdb/cockroach:v24.1.0 start-single-node --insecure
docker exec cockroachdb cockroach sql --insecure --execute 'CREATE DATABASE maestro;'
```
