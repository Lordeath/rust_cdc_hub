FROM debian:stable-20251117
COPY "target/release/rust_cdc_hub" "/app/rust_cdc_hub"
ENTRYPOINT ["/app/rust_cdc_hub"]
