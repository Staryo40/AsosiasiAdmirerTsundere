# syntax=docker/dockerfile:1

# Build stage: compiles the service into one runnable jar.
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace

# The Gradle files come first, so downloaded dependencies stay cached until they change.
COPY gradlew settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle gradle
RUN ./gradlew --no-daemon dependencies > /dev/null

COPY src src
RUN ./gradlew --no-daemon shadowJar

# Runtime stage: only a JRE and the jar. No configuration or secrets are baked in;
# everything arrives as environment variables when the container starts.
FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S pvmbg && adduser -S -G pvmbg pvmbg
WORKDIR /app
COPY --from=build --chown=pvmbg:pvmbg /workspace/build/libs/pvmbg-all.jar pvmbg.jar
USER pvmbg
EXPOSE 8082

# Exec form, so the JVM is PID 1 and receives `docker stop`'s signal directly. Arguments after the
# image name are passed to the service (e.g. `docker run pvmbg 10` seeds 10 initial records).
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "pvmbg.jar"]
