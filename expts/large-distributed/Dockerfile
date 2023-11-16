# Stage 1
FROM rust:latest AS builder
WORKDIR /usr/src/parsimon
COPY . .
RUN rustup override set nightly
RUN cargo build --release --bin large-distributed
RUN cargo build --release --bin worker

# Stage 2
FROM debian:bullseye
COPY --from=builder /usr/src/parsimon/target/release/worker /usr/local/bin/parsimon-worker
COPY --from=builder /usr/src/parsimon/target/release/large-distributed /usr/local/bin/parsimon-manager
COPY ./expts/large-distributed/spec /spec
EXPOSE 2727
COPY ./expts/large-distributed/entrypoint.sh /
RUN chmod +x /entrypoint.sh
COPY ./expts/large-distributed/run.sh /
RUN chmod +x /run.sh
ENV MODE=manager
ENTRYPOINT ["/entrypoint.sh"]
