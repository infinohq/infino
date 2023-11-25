# Image to build infino release binary.
FROM rust:1.67 as builder

WORKDIR /usr/src/infino
COPY . .
RUN cargo build --release

# Smaller image for running infino.
FROM debian:bullseye
WORKDIR /opt/infino
RUN apt-get update & apt-get install -y extra-runtime-dependencies & rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/infino/target/release/infino /opt/infino/infino
COPY --from=builder /usr/src/infino/config /opt/infino/config

# By default, infino server starts on port 3000.
EXPOSE 3000/tcp

# Start infino server.
CMD ["./infino"]
