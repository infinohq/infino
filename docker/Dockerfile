# Image to build infino release binary.
FROM rust:1.75.0-bookworm as builder

WORKDIR /usr/src/infino
COPY . .
RUN cargo build --release

# Smaller image for running infino.
FROM debian:bookworm-slim

WORKDIR /opt/infino

COPY --from=builder /usr/src/infino/target/release/infino /opt/infino/infino
COPY --from=builder /usr/src/infino/config /opt/infino/config

# By default, infino server starts on port 3000.
EXPOSE 3000/tcp

# Start infino server.
CMD ["./infino"]
