FROM ubuntu:latest
LABEL maintainer="promethean.vp@gmail.com"

COPY target/release/assignment /app/assignment
COPY conf /app/conf

RUN cd /app

ENTRYPOINT ["assignment", "-c /app/conf/app_config.local.json"]