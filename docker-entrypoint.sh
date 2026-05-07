#!/bin/sh
set -eu

if [ "${1#-}" != "$1" ]; then
    set -- /app/rust_cdc_hub "$@"
fi

if [ "$(id -u)" = "0" ]; then
    mkdir -p /checkpoint
    chown -R appuser:appuser /checkpoint
    exec gosu appuser "$@"
fi

exec "$@"
