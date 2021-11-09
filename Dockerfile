FROM rust:1.56.1 as builder

## Optimizing build time by caching in layers the result of compiling project dependencies
RUN USER=root cargo new --bin buska
WORKDIR /buska

RUN apt-get update && apt-get install -y cmake

# Copy manifests
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml
# Sub-Projects
RUN mkdir -p cli/src
COPY ./cli/Cargo.toml ./cli/Cargo.toml
RUN mkdir -p core/src
COPY ./core/Cargo.toml ./core/Cargo.toml

RUN cp src/main.rs cli/src/
RUN cp src/main.rs core/src/

# Build with no sources, so that it caches dependencies
RUN cargo build --all --release
RUN rm -rf src

# Copy sources in order to build the proper release
COPY ./src ./src
COPY ./cli/src ./cli/src
COPY ./core/src ./core/src

# Build the release
RUN rm -f ./target/release/deps/buska*
RUN cargo build --all --release

# Copy executable file to safe directory
RUN mkdir -p /build-out
RUN cp target/release/buska-cli /build-out/

FROM ubuntu:latest
WORKDIR /opt

# Setting environment variables

# Install mandatory packages
RUN apt-get update && apt-get install -y libssl1.1 openssl ca-certificates


# Get the executable file from previous build
COPY --from=builder /build-out/buska-cli /opt

ENTRYPOINT ["/opt/buska-cli"]