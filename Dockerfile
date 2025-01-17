FROM rust:1.74-slim as builder

WORKDIR /app
COPY . .

RUN cargo build --release

FROM debian:bookworm-slim

WORKDIR /data

# Install necessary runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary
COPY --from=builder /app/target/release/processor /usr/local/bin/

ENV RUST_LOG=info
ENV APP_DATA_DIR=/data

# Create data directory structure
RUN mkdir -p /data/in/tables /data/in/files /data/out/tables /data/out/files

ENTRYPOINT ["processor", "--data-dir", "/data"]
