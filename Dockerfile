# Build exporter
ARG BASE_OS_VERSION=bullseye
FROM python:3.9.16-$BASE_OS_VERSION AS exporter-builder

WORKDIR /usr/src/

COPY scripts/entrypoint.sh /usr/bin/
RUN chmod +x /usr/bin/entrypoint.sh
ADD https://github.com/wal-g/wal-g/releases/download/v2.0.1/wal-g-pg-ubuntu-20.04-amd64.tar.gz .
RUN tar -zxvf wal-g-pg-ubuntu-20.04-amd64.tar.gz \
    && mv ./wal-g-pg-ubuntu-20.04-amd64 /usr/bin/wal-g
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY exporter.py .
RUN pyinstaller --name exporter \
    --onefile exporter.py \
    && mv dist/exporter wal-g-prometheus-exporter

# Build final image
FROM debian:$BASE_OS_VERSION-slim

RUN apt-get update -qq \
    && apt-get install -qqy \
        ca-certificates \
        daemontools \
    && apt-get -y -q autoclean \
    && apt-get -y -q autoremove \
    && apt-get clean \
    && rm -rf \
        /tmp/* \
        /usr/share/doc/* \
        /var/lib/apt/lists/* \
        /var/tmp/*

COPY --from=exporter-builder /usr/bin/entrypoint.sh /usr/src/wal-g-prometheus-exporter /usr/bin/wal-g /usr/bin/

ENTRYPOINT ["/usr/bin/entrypoint.sh"]
