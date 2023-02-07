FROM rustlang/rust:nightly-bullseye-slim as chef
RUN cargo +nightly install cargo-chef --locked
WORKDIR app

FROM chef AS planner
COPY . .
RUN cargo +nightly chef prepare --recipe-path recipe.json

FROM chef AS builder

COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN apt update
RUN apt install -y cmake
RUN apt install -y libssl-dev
RUN cargo +nightly chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo +nightly build --all --release
RUN apt-get update && apt-get install -y cmake

FROM debian:bullseye-slim AS runtime
WORKDIR app
RUN apt update
RUN apt install -y libssl1.1 openssl ca-certificates
COPY --from=builder /app/target/release/buska-cli /usr/local/bin

ENTRYPOINT ["/usr/local/bin/buska-cli"]