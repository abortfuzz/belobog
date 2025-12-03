FROM rust:bookworm as builder

WORKDIR /work
COPY Cargo.lock .
COPY Cargo.toml .
COPY crates ./crates

RUN apt update && apt install libclang-dev -y
RUN rustup default 1.88
RUN cargo build --release

FROM rust:bookworm as runner

COPY --from=builder /work/target/release/belobog-cli /usr/bin/belobog-cli

ENTRYPOINT [ "/usr/bin/belobog-cli" ]