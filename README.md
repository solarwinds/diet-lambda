# diet-lambda

This is a lightweight alternative to the SolarWinds distribution of the OpenTelemetry collector for AWS Lambda. It can be used as a drop-in replacement for most use-cases, while weighting only a few megabytes. As such, it can speed up cold start times as there is less data to download to the environment, and uses less memory.

The collector listens for `HTTP/OTLP`, `HTTP/JSON` and `gRPC/OTLP` traces, metrics, logs and profiles on ports `4317` and `4318`. It also collects logs output from the Lambda function, and will attempt to correlate them with the request trace whenever possible. It supports both classic and managed Lambda instances.

## Configuration

This collector uses the same `SW_APM_API_TOKEN` and `SW_APM_DATA_CENTER` environment variables as the full collector. It does not support configuration via a `yaml` file, or any custom processing logic.

## Docker Images

These are also updated on new commits to main. The collector is built on Amazon Linux and expects OpenSSL to be available in the image.

```dockerfile
FROM ghcr.io/solarwinds/diet-lambda AS collector
# ...
COPY --from=collector /opt/extensions/diet-lambda /opt/extensions/diet-lambda
# ...
```

## Staging ARNs

These are updated on new commits to `main`, or manually by triggering CI workflow.

- `arn:aws:lambda:us-east-1:858939916050:layer:diet-lambda-x86_64`
- `arn:aws:lambda:us-east-1:858939916050:layer:diet-lambda-aarch64`

## Developing

The collector requires a nightly Rust toolchain in order to build the standard library while optimizing for a small binary size. It uses the standard Cargo workflow, just run `cargo build`. `rustfmt` and `clippy` are run in CI, which will fail if there are any warnings.

The primary goal of this collector is to be lightweight, as such dependencies should be vetted and kept to a minimum, and niche features should be kept to the Go collector if they might bloat this collector for people who do not need them.
