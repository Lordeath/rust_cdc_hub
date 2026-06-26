# =========================
# 1. 构建阶段
# =========================
ARG RUST_IMAGE=rust:1-bullseye
ARG RUNTIME_IMAGE=debian:bullseye-20260421
ARG DEBIAN_MIRROR=mirrors.aliyun.com
ARG CARGO_REGISTRY=sparse+https://rsproxy.cn/index/
FROM ${RUST_IMAGE} AS builder

WORKDIR /app
ARG CARGO_REGISTRY

# ---------- 使用国内 Rust / Cargo 镜像 ----------
RUN mkdir -p /usr/local/cargo \
    && { \
        echo '[source.crates-io]'; \
        echo 'replace-with = "cargo-mirror"'; \
        echo '[source.cargo-mirror]'; \
        echo "registry = \"${CARGO_REGISTRY}\""; \
        echo '[net]'; \
        echo 'git-fetch-with-cli = true'; \
    } > /usr/local/cargo/config.toml

# ---------- 拷贝真实源码 ----------
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY common ./common
COPY mysql_to_dameng ./mysql_to_dameng
COPY plugin ./plugin
COPY sink ./sink
COPY source ./source

# ---------- 正式编译 ----------
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,id=rust-cdc-hub-target,target=/app/target,sharing=locked \
    cargo build --release \
    && cp /app/target/release/rust_cdc_hub /tmp/rust_cdc_hub \
    && strip /tmp/rust_cdc_hub

# =========================
# 2. 运行阶段
# =========================
FROM ${RUNTIME_IMAGE}

WORKDIR /app

ENV TZ=Asia/Shanghai
ENV RUN_AS_USER=root
ARG DEBIAN_MIRROR

RUN sed -i "s|http://deb.debian.org/debian-security|http://${DEBIAN_MIRROR}/debian-security|g; s|http://deb.debian.org/debian|http://${DEBIAN_MIRROR}/debian|g; s|http://security.debian.org/debian-security|http://${DEBIAN_MIRROR}/debian-security|g; s|http://security.debian.org/debian|http://${DEBIAN_MIRROR}/debian-security|g" /etc/apt/sources.list /etc/apt/sources.list.d/* 2>/dev/null || true \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ca-certificates gosu tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin appuser \
    && mkdir -p /checkpoint \
    && chown -R appuser:appuser /app /checkpoint

# 拷贝最终二进制
COPY --from=builder --chown=appuser:appuser /tmp/rust_cdc_hub /app/rust_cdc_hub
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["/app/rust_cdc_hub"]
