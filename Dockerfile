# Multi-stage Docker build for Redis Java Clone

# Stage 1: Build & Compile
FROM eclipse-temurin:21-jdk-alpine AS builder

WORKDIR /build

# Copy source and pom
COPY src ./src
COPY pom.xml ./

# Compile Java source files
RUN mkdir -p bin && \
    find src -name "*.java" > sources.txt && \
    javac -d bin @sources.txt

# Stage 2: Lightweight Runtime
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

# Non-root user for security
RUN addgroup -S redisgroup && adduser -S redisuser -G redisgroup

# Copy compiled binaries from builder stage
COPY --from=builder /build/bin ./bin

# Expose default Redis port
EXPOSE 6379

USER redisuser

# Default startup command
ENTRYPOINT ["java", "-cp", "bin", "com.redisclone.server.RedisServer"]
CMD ["--port", "6379"]
