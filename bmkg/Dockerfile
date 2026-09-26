# syntax=docker/dockerfile:1

# `cargo-chef` splits the build into a dependency layer and a source layer, so `docker build` only
# recompiles BMKG's own ~2000 lines when only src/ changes, not every dependency crate every time.
FROM rust:1-alpine AS chef
RUN apk add --no-cache musl-dev
RUN cargo install cargo-chef --locked
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin bmkg

# Runtime stage: only the compiled binary. No configuration or secrets are baked in; everything
# arrives as environment variables when the container starts. `#[cfg(test)]` code never makes it
# into a `--release` binary regardless, so unlike PVMBG there is no separate test directory to
# exclude from the build context (see .dockerignore).
FROM alpine:3.20
RUN addgroup -S bmkg && adduser -S -G bmkg bmkg
WORKDIR /app
COPY --from=builder /app/target/release/bmkg ./bmkg
USER bmkg
EXPOSE 8081

# Exec form, so the process is PID 1 and receives `docker stop`'s signal directly. Arguments after
# the image name are passed to the service (e.g. `docker run bmkg 10` seeds 10 initial records).
ENTRYPOINT ["./bmkg"]
