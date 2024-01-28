# Image to build infino release binary.
FROM rust:1.75.0-bookworm as builder

WORKDIR /usr/src/infino
COPY . .

# The built-in libgit in cargo can lead to out of memory
# errors on low memory machines. If git is installed then
# use the system git by setting CARGO_NET_GIT_FETCH_WITH_CLI=true,
# otherwis let cargo use its libgit.
RUN command git -v > /dev/null 2>&1 && \
    CARGO_NET_GIT_FETCH_WITH_CLI=true cargo build --release || \
    cargo build --release

# Smaller image for running infino.
FROM debian:bookworm-slim

# Install libssl shared library as infino depends on it and
# libssl3 is not available in the debian:bookworm-slim image.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive && \
    apt-get install -y libssl3 && \
    # Cleanup after the installs to remove the apt cache and any temporary files
    apt-get autoremove -y && \
    apt-get clean -y && \
    rm -fr /var/lib/apt/lists/*

WORKDIR /opt/infino

COPY --from=builder /usr/src/infino/target/release/infino /opt/infino/infino
COPY --from=builder /usr/src/infino/config /opt/infino/config

# By default, infino server starts on port 3000.
EXPOSE 3000/tcp

# Start infino server.
CMD ["./infino"]
