# 基于 influxdata-docker/influxdb/3.8-core，用 CI 产出的 tarball 构建（不 curl 官方源）
FROM ubuntu:24.04

RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get update && \
    apt-get install --no-install-recommends -y \
        ca-certificates \
        gettext-base \
        libssl3 && \
    rm -rf /var/lib/apt/lists*

RUN groupadd --gid 1500 influxdb3 && \
    useradd --uid 1500 --gid influxdb3 --shell /bin/bash --create-home influxdb3 && \
    mkdir -p /var/lib/influxdb3 /usr/lib/influxdb3 /plugins

# CI 将 tarball 以固定名传入构建上下文
ARG TARBALL=influxdb3-core.tar.gz
COPY ${TARBALL} /tmp/influxdb3-core.tar.gz
RUN tar --strip-components 1 -C /usr/lib/influxdb3 -xzf /tmp/influxdb3-core.tar.gz && \
    mv /usr/lib/influxdb3/influxdb3 /usr/bin/influxdb3 && \
    chown -R influxdb3:influxdb3 /var/lib/influxdb3 /plugins && \
    chown -R root:root /usr/lib/influxdb3 && \
    rm /tmp/influxdb3-core.tar.gz

COPY entrypoint.sh /usr/bin/entrypoint.sh
RUN chmod +x /usr/bin/entrypoint.sh

USER influxdb3
RUN mkdir -p ~/.influxdb3

ENV INFLUXDB3_PLUGIN_DIR=/plugins
ENV INFLUXDB3_DATA_DIR=/home/influxdb3/.influxdb3
ENV INFLUXDB3_SERVE_INVOCATION_METHOD=docker-hub
ENV INFLUXDB_IOX_DB_DIR=/var/lib/influxdb3
ENV LOG_FILTER=info

EXPOSE 8181

ENTRYPOINT ["/usr/bin/entrypoint.sh"]
CMD ["influxdb3", "serve"]
