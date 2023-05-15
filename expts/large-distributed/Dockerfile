# Stage 1
FROM rust:latest AS builder
WORKDIR /usr/src/parsimon
COPY . .
RUN rustup override set nightly
RUN cargo build --release

# Stage 2
FROM debian:bullseye
COPY --from=builder /usr/src/parsimon/target/release/worker /usr/local/bin/parsimon-worker
COPY --from=builder /usr/src/parsimon/target/release/large-distributed /usr/local/bin/parsimon-manager
EXPOSE 2727
COPY ./entrypoint.sh /
RUN chmod +x /entrypoint.sh
# Set the entrypoint to the script with CMD arguments
ENTRYPOINT ["/entrypoint.sh"]
CMD ["parsimon-worker"]
