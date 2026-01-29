use axum::{Json, Router, extract::State, response::IntoResponse, routing::post};
use lambda_extension::{LambdaTelemetry, LambdaTelemetryRecord};
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::InstrumentationScope,
    logs::v1::{ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use tokio::sync::mpsc;

use crate::{ServiceRequest, env::Config};

pub fn router() -> Router<mpsc::UnboundedSender<ServiceRequest>> {
    Router::new().route(Config::TELEMETRY_ROUTE, post(telemetry))
}

async fn telemetry(
    State(tx): State<mpsc::UnboundedSender<ServiceRequest>>,
    Json(events): Json<Vec<LambdaTelemetry>>,
) -> impl IntoResponse {
    let mut flushes = Vec::new();
    let mut logs = Vec::new();

    for event in events {
        match event.record {
            LambdaTelemetryRecord::PlatformRuntimeDone { request_id, .. } => {
                flushes.push(request_id);
            }
            LambdaTelemetryRecord::Function(record) => {
                if let Some(log) = crate::logs::parse(record, event.time) {
                    logs.push(log);
                }
            }
            _ => continue,
        }
    }

    if !logs.is_empty() {
        let _ = tx.send(ServiceRequest::Logs(ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource::default()),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: env!("CARGO_PKG_NAME").to_string(),
                        version: env!("CARGO_PKG_VERSION").to_string(),
                        ..Default::default()
                    }),
                    log_records: logs,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }));
    }

    for id in flushes {
        let _ = tx.send(ServiceRequest::Flush(id));
    }
}
