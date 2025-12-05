#!/bin/bash

for run in {1..30}; do
  echo "waiting /run/etc/wal-e.d/env to come up... retry $run"
  ls /run/etc/wal-e.d/env
  if find /run/etc/wal-e.d/env -mindepth 1 -maxdepth 1 | read; then
    echo "/run/etc/wal-e.d/env is populated, starting now..."
    break
  fi
  sleep 6
done
exec envdir /run/etc/wal-e.d/env wal-g-prometheus-exporter
