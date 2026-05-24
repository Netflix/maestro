FROM openjdk:21-slim as builder
ARG JAR_FILE=build/libs/maestro-server.jar
COPY ${JAR_FILE} application.jar
RUN java -Djarmode=layertools -jar application.jar extract

FROM openjdk:21-slim
COPY --from=builder dependencies/ ./
COPY --from=builder snapshot-dependencies/ ./
COPY --from=builder spring-boot-loader/ ./
COPY --from=builder application/ ./
ENTRYPOINT ["java", "-Djava.security.manager=allow", "org.springframework.boot.loader.JarLauncher"]