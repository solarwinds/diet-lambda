use anyhow::Error;
use axum::Router;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tower_http::decompression::DecompressionLayer;

use crate::{ServiceRequest, env::Config, util::MultiListener};

mod grpc;
mod http;
mod telemetry;

pub async fn task(
    tx: mpsc::UnboundedSender<ServiceRequest>,
    token: CancellationToken,
) -> Result<(), Error> {
    let router = Router::new()
        .merge(telemetry::router())
        .merge(http::router())
        .with_state(tx.clone())
        .layer(DecompressionLayer::new().gzip(true))
        .merge(grpc::router(tx));

    let listener = MultiListener::bind(Config::BIND_ADDRESSES).await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(token.cancelled_owned())
        .await?;

    Ok(())
}
