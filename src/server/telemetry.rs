use axum::{Json, Router, extract::State, response::IntoResponse, routing::post};
use lambda_extension::{LambdaTelemetry, LambdaTelemetryRecord};
use tokio::sync::mpsc;

use crate::{ServiceRequest, env::Config};

pub fn router() -> Router<mpsc::UnboundedSender<ServiceRequest>> {
    Router::new().route(Config::TELEMETRY_ROUTE, post(telemetry))
}

async fn telemetry(
    State(tx): State<mpsc::UnboundedSender<ServiceRequest>>,
    Json(events): Json<Vec<LambdaTelemetry>>,
) -> impl IntoResponse {
    for event in events {
        match event.record {
            LambdaTelemetryRecord::PlatformRuntimeDone { request_id, .. } => {
                let _ = tx.send(ServiceRequest::Flush(request_id));
            }
            _ => continue,
        }
    }
}
