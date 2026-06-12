use axum::{
    Router,
    body::{Body, Bytes},
    extract::{FromRequest, Request, State},
    response::{IntoResponse, Response},
    routing::post,
};
use hyper::{StatusCode, header::CONTENT_TYPE};
use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::{ExportLogsServiceRequest, ExportLogsServiceResponse},
        metrics::v1::{ExportMetricsServiceRequest, ExportMetricsServiceResponse},
        profiles::v1development::{ExportProfilesServiceRequest, ExportProfilesServiceResponse},
        trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse},
    },
    trace::v1::Status,
};
use prost::Message;
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::mpsc;
use tower_http::decompression::DecompressionLayer;

use crate::{ServiceRequest, env::Config};

pub fn router() -> Router<mpsc::UnboundedSender<ServiceRequest>> {
    Router::new()
        .route(Config::TRACES_ROUTE, post(trace))
        .route(Config::METRICS_ROUTE, post(metrics))
        .route(Config::LOGS_ROUTE, post(logs))
        .route(Config::PROFILES_ROUTE, post(profiles))
        .layer(DecompressionLayer::new().gzip(true).zstd(true))
}

async fn trace(
    State(tx): State<mpsc::UnboundedSender<ServiceRequest>>,
    Otlp { message, ty }: Otlp<ExportTraceServiceRequest>,
) -> Otlp<ExportTraceServiceResponse> {
    let _ = tx.send(ServiceRequest::Trace(message));
    Otlp {
        message: ExportTraceServiceResponse::default(),
        ty,
    }
}

async fn metrics(
    State(tx): State<mpsc::UnboundedSender<ServiceRequest>>,
    Otlp { message, ty }: Otlp<ExportMetricsServiceRequest>,
) -> Otlp<ExportMetricsServiceResponse> {
    let _ = tx.send(ServiceRequest::Metrics(message));
    Otlp {
        message: ExportMetricsServiceResponse::default(),
        ty,
    }
}

async fn logs(
    State(tx): State<mpsc::UnboundedSender<ServiceRequest>>,
    Otlp { message, ty }: Otlp<ExportLogsServiceRequest>,
) -> Otlp<ExportLogsServiceResponse> {
    let _ = tx.send(ServiceRequest::Logs(message));
    Otlp {
        message: ExportLogsServiceResponse::default(),
        ty,
    }
}

async fn profiles(
    State(tx): State<mpsc::UnboundedSender<ServiceRequest>>,
    Otlp { message, ty }: Otlp<ExportProfilesServiceRequest>,
) -> Otlp<ExportProfilesServiceResponse> {
    let _ = tx.send(ServiceRequest::Profiles(message));
    Otlp {
        message: ExportProfilesServiceResponse::default(),
        ty,
    }
}

struct Otlp<T> {
    message: T,
    ty: ContentType,
}

struct OtlpError<E> {
    error: E,
    status: StatusCode,
    ty: ContentType,
}

#[derive(Copy, Clone)]
enum ContentType {
    Protobuf,
    Json,
}

impl<T: Message + DeserializeOwned + Default, S: Send + Sync> FromRequest<S> for Otlp<T> {
    type Rejection = Response;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let ty = match req.headers().get(CONTENT_TYPE) {
            Some(name) if name == "application/x-protobuf" => ContentType::Protobuf,
            Some(name) if name == "application/json" => ContentType::Json,

            _ => {
                return Err(Response::builder()
                    .status(StatusCode::UNSUPPORTED_MEDIA_TYPE)
                    .body(Body::empty())
                    .unwrap());
            }
        };

        let bytes = Bytes::from_request(req, state)
            .await
            .map_err(|err| error(err, StatusCode::INTERNAL_SERVER_ERROR, ty))?;

        let message = match ty {
            ContentType::Protobuf => {
                T::decode(bytes).map_err(|err| error(err, StatusCode::BAD_REQUEST, ty))
            }
            ContentType::Json => serde_json::from_slice(&bytes)
                .map_err(|err| error(err, StatusCode::BAD_REQUEST, ty)),
        }?;

        Ok(Otlp { message, ty })
    }
}

impl<T: Message + Serialize> IntoResponse for Otlp<T> {
    fn into_response(self) -> Response {
        match self.ty {
            ContentType::Protobuf => Response::builder()
                .header(CONTENT_TYPE, "application/x-protobuf")
                .body(Body::from(self.message.encode_to_vec()))
                .unwrap(),

            ContentType::Json => Response::builder()
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&self.message).unwrap()))
                .unwrap(),
        }
    }
}

impl<E: std::error::Error> IntoResponse for OtlpError<E> {
    fn into_response(self) -> Response {
        tracing::warn!(err = %self.error, "http exporter error");

        let status = Status {
            message: self.error.to_string(),
            code: 2,
        };

        match self.ty {
            ContentType::Protobuf => Response::builder()
                .status(self.status)
                .header(CONTENT_TYPE, "application/x-protobuf")
                .body(Body::from(status.encode_to_vec()))
                .unwrap(),

            ContentType::Json => Response::builder()
                .status(self.status)
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&status).unwrap()))
                .unwrap(),
        }
    }
}

fn error(err: impl std::error::Error, status: StatusCode, ty: ContentType) -> Response {
    OtlpError {
        error: err,
        status,
        ty,
    }
    .into_response()
}
