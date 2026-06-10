# syntax=docker/dockerfile:1

# ---- Build stage: compile the Spring Boot fat jar ----
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src

# Copy the full source tree and build the runnable (fat) jar, skipping tests.
COPY . .
RUN chmod +x gradlew \
    && ./gradlew :maestro-server:bootJar --no-daemon -x test \
    && cp "$(find maestro-server/build/libs -name '*.jar' ! -name '*-plain.jar' | head -n1)" /app.jar

# ---- Runtime stage: slim JRE with just the jar ----
FROM eclipse-temurin:21-jre
WORKDIR /app

# Non-root runtime user.
RUN useradd --system --uid 10001 maestro
COPY --from=build /app.jar /app/app.jar

ENV JAVA_OPTS=""
EXPOSE 8080
USER maestro

# Actuator health is enabled by default (spring-boot-starter-actuator is on the classpath).
HEALTHCHECK --interval=15s --timeout=3s --start-period=60s --retries=5 \
    CMD ["sh", "-c", "wget -qO- http://127.0.0.1:8080/actuator/health | grep -q UP || exit 1"]

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
