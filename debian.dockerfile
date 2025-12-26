# =========================
# 1. 构建阶段
# =========================
FROM rust:1.92-bookworm AS builder

WORKDIR /app

# ---------- 使用字节跳动 Rust / Cargo 镜像 ----------
# crates.io -> mirrors.volces.com
RUN mkdir -p /root/.cargo \
 && echo '[source.crates-io]' > /root/.cargo/config.toml \
 && echo 'replace-with = "volces"' >> /root/.cargo/config.toml \
 && echo '[source.volces]' >> /root/.cargo/config.toml \
 && echo 'registry = "https://mirrors.volces.com/crates.io-index"' >> /root/.cargo/config.toml

# ---------- 依赖缓存优化 ----------
COPY Cargo.toml Cargo.lock ./

# ---------- 拷贝真实源码 ----------
COPY src ./src
COPY common ./common
COPY plugin ./plugin
COPY sink ./sink
COPY source ./source

# ---------- 正式编译 ----------
RUN cargo build --release

# =========================
# 2. 运行阶段
# =========================
FROM debian:bookworm-20251208

WORKDIR /app

# 拷贝最终二进制
COPY --from=builder /app/target/release/rust_cdc_hub /app/rust_cdc_hub

# 非 root 运行
RUN useradd -r -s /sbin/nologin appuser \
 && chown appuser:appuser /app/rust_cdc_hub

USER appuser

ENTRYPOINT ["/app/rust_cdc_hub"]
