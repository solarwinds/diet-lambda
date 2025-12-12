use std::sync::Arc;

use anyhow::Error;
use http_body_util::{BodyExt, Empty};
use hyper::{
    Request,
    header::{AUTHORIZATION, USER_AGENT},
};
use serde::Deserialize;
use tokio::{
    fs,
    time::{self, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

use crate::{
    env::Config,
    util::{Client, body},
};

async fn fetch(client: &Client, config: &Config) -> Result<Vec<u8>, Error> {
    let response = client
        .request(
            Request::builder()
                .method("GET")
                .uri(&config.urls.settings)
                .header(AUTHORIZATION, format!("Bearer {}", config.token))
                .header(USER_AGENT, Config::USER_AGENT)
                .body(body(Empty::new()))?,
        )
        .await?;

    let body = response.into_body().collect().await?.to_bytes();

    // We only need to parse the warning field, and everything else should
    // be passed as is. That way we won't ever drop new fields that may be
    // added in the future.
    #[derive(Deserialize)]
    struct Settings {
        #[serde(default)]
        warning: Option<String>,
    }
    let Settings { warning } = serde_json::from_slice(&body)?;

    if let Some(warning) = warning {
        eprintln!("{warning}");
    }

    // Libraries expect this file to be an array of settings objects
    let mut contents = Vec::with_capacity(body.len() + 2);
    contents.push(b'[');
    contents.extend_from_slice(&body);
    contents.push(b']');

    Ok(contents)
}

pub async fn task(
    config: Arc<Config>,
    client: Client,
    token: CancellationToken,
) -> Result<(), Error> {
    let mut timer = time::interval(Config::SETTINGS_INTERVAL);
    timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        if token.run_until_cancelled(timer.tick()).await.is_none() {
            // We have been cancelled
            break Ok(());
        }

        let contents = token.run_until_cancelled(fetch(&client, &config)).await;
        match contents {
            Some(Ok(contents)) => {
                if let Err(err) = fs::write(Config::SETTINGS_PATH, contents).await {
                    eprintln!("failed to write sampling settings: {err}");
                }
            }
            Some(Err(err)) => {
                eprintln!("failed to fetch sampling settings: {err}");
            }
            None => {
                // We have been cancelled
                break Ok(());
            }
        }
    }
}
