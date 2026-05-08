#!/bin/sh
set -eu

if [ "${1#-}" != "$1" ]; then
    set -- /app/rust_cdc_hub "$@"
fi

if [ "$(id -u)" = "0" ]; then
    mkdir -p /checkpoint

    if [ "${RUN_AS_USER:-root}" != "root" ]; then
        chown -R "${RUN_AS_USER}:${RUN_AS_USER}" /checkpoint
        exec gosu "${RUN_AS_USER}" "$@"
    fi
fi

exec "$@"
