use anyhow::{Context, Error};
use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full};
use hyper::Request;
use lambda_extension::NextEvent;
use serde::{Deserialize, Serialize};

use crate::{
    env::Config,
    util::{Client, body},
};

#[tracing::instrument(level = "debug", err, skip_all)]
pub async fn register(client: &Client, config: &Config) -> Result<(String, Option<String>), Error> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Response {
        account_id: String,
    }

    let data = if config.managed {
        r#"{ "events": ["SHUTDOWN"] }"#
    } else {
        r#"{ "events": ["INVOKE", "SHUTDOWN"] }"#
    };

    let response = client
        .request(
            Request::builder()
                .method("POST")
                .uri(&config.urls.extension.register)
                .header("Lambda-Extension-Name", &config.executable)
                .header("Lambda-Extension-Accept-Feature", "accountId")
                .body(body(Full::new(Bytes::from_static(data.as_bytes()))))?,
        )
        .await
        .context("registration request failed")?;

    let id = response
        .headers()
        .get(Config::ID_HEADER)
        .context("registration response invalid")?
        .to_str()
        .context("registration response invalid")?
        .to_string();

    let body = response.into_body().collect().await?.to_bytes();
    let account_id = match serde_json::from_slice(&body) {
        Ok(Response { account_id }) => Some(account_id),
        _ => None,
    };

    Ok((id, account_id))
}

#[tracing::instrument(level = "debug", err, skip_all)]
pub async fn next(client: &Client, config: &Config, id: &str) -> Result<NextEvent, Error> {
    let response = client
        .request(
            Request::builder()
                .method("GET")
                .uri(&config.urls.extension.event)
                .header(Config::ID_HEADER, id)
                .body(body(Empty::new()))?,
        )
        .await
        .context("next event request failed")?;

    let body = response.into_body().collect().await?.to_bytes();
    let event = serde_json::from_slice(&body).context("invalid next event response")?;
    Ok(event)
}

pub async fn error(
    client: &Client,
    config: &Config,
    id: &str,
    error: &Error,
    init: bool,
) -> Result<(), Error> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Payload<'a> {
        error_message: &'a str,
        error_type: &'a str,
        stack_trace: &'a [String],
    }

    let url = if init {
        &config.urls.extension.exit_error
    } else {
        &config.urls.extension.init_error
    };

    let error_message = error.to_string();
    let stack_trace: Vec<String> = error.chain().map(|err| err.to_string()).collect();
    let payload = Payload {
        error_message: &error_message,
        error_type: "Error",
        stack_trace: &stack_trace,
    };

    let data = serde_json::to_vec(&payload)?;

    client
        .request(
            Request::builder()
                .method("POST")
                .uri(url)
                .header(Config::ID_HEADER, id)
                .body(body(Full::new(Bytes::from_owner(data))))?,
        )
        .await
        .context("error report request failed")?;

    Ok(())
}
