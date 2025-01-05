FROM rust
ENV PROTOC=/app/bin/protoc
ENV PORT=8080
LABEL maintainer="promethean.vp@gmail.com"
WORKDIR /app
COPY . .
RUN wget https://github.com/protocolbuffers/protobuf/releases/download/v29.2/protoc-29.2-linux-x86_64.zip
RUN unzip protoc-29.2-linux-x86_64.zip
RUN cargo test
RUN cargo build -r
EXPOSE 8080
ENTRYPOINT /app/target/release/assignment -u ${ETCD_URL}