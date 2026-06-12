use anyhow::{Context, Error};
use bytes::Bytes;
use http_body_util::Full;
use hyper::Request;

use crate::{
    env::Config,
    util::{Client, body},
};

#[tracing::instrument(level = "debug", err, skip_all)]
pub async fn register(client: &Client, config: &Config, id: &str) -> Result<(), Error> {
    let data = format!(
        r#"{{
            "schemaVersion": "2025-01-29",
            "types": ["platform", "function"],
            "buffering": {{
                "maxItems": 10000,
                "maxBytes": 262144,
                "timeoutMs": 25
            }},
            "destination": {{
                "protocol": "HTTP",
                "URI": "{endpoint}"
            }}
        }}"#,
        endpoint = config.urls.telemetry.endpoint,
    );

    client
        .request(
            Request::builder()
                .method("PUT")
                .uri(&config.urls.telemetry.register)
                .header(Config::ID_HEADER, id)
                .body(body(Full::new(Bytes::from(data))))?,
        )
        .await
        .context("telemetry registration request failed")?;

    Ok(())
}
