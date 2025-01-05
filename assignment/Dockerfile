FROM rust
ENV PROTOC=/app/bin/protoc
LABEL maintainer="promethean.vp@gmail.com"
WORKDIR /app
COPY . .
RUN wget https://github.com/protocolbuffers/protobuf/releases/download/v29.2/protoc-29.2-linux-x86_64.zip
RUN unzip protoc-29.2-linux-x86_64.zip
RUN cargo test -r
RUN cargo build -r

FROM rust
WORKDIR /app
COPY --from=0 /app/target/release/assignment /app
COPY --from=0 /app/log4rs.yml /app
ENV PORT=8080
EXPOSE 8080
ENTRYPOINT /app/assignment -u ${ETCD_URL}