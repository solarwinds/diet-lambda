use std::sync::Arc;

use axum::Router;
use opentelemetry_proto::tonic::collector::{
    logs::v1::{
        ExportLogsServiceRequest, ExportLogsServiceResponse,
        logs_service_server::{LogsService, LogsServiceServer},
    },
    metrics::v1::{
        ExportMetricsServiceRequest, ExportMetricsServiceResponse,
        metrics_service_server::{MetricsService, MetricsServiceServer},
    },
    profiles::v1development::{
        ExportProfilesServiceRequest, ExportProfilesServiceResponse,
        profiles_service_server::{ProfilesService, ProfilesServiceServer},
    },
    trace::v1::{
        ExportTraceServiceRequest, ExportTraceServiceResponse,
        trace_service_server::{TraceService, TraceServiceServer},
    },
};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status, codec::CompressionEncoding, service::Routes};

use crate::ServiceRequest;

pub fn router(tx: mpsc::UnboundedSender<ServiceRequest>) -> Router {
    let service = Arc::new(OtlpService { tx });

    let mut builder = Routes::builder();
    builder.add_service(
        TraceServiceServer::from_arc(service.clone()).accept_compressed(CompressionEncoding::Gzip),
    );
    builder.add_service(
        MetricsServiceServer::from_arc(service.clone())
            .accept_compressed(CompressionEncoding::Gzip),
    );
    builder.add_service(
        LogsServiceServer::from_arc(service.clone()).accept_compressed(CompressionEncoding::Gzip),
    );
    builder.add_service(
        ProfilesServiceServer::from_arc(service).accept_compressed(CompressionEncoding::Gzip),
    );

    builder.routes().into_axum_router()
}

struct OtlpService {
    tx: mpsc::UnboundedSender<ServiceRequest>,
}

#[tonic::async_trait]
impl TraceService for OtlpService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let _ = self.tx.send(ServiceRequest::Trace(request.into_inner()));
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl MetricsService for OtlpService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let _ = self.tx.send(ServiceRequest::Metrics(request.into_inner()));
        Ok(Response::new(ExportMetricsServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl LogsService for OtlpService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let _ = self.tx.send(ServiceRequest::Logs(request.into_inner()));
        Ok(Response::new(ExportLogsServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl ProfilesService for OtlpService {
    async fn export(
        &self,
        request: Request<ExportProfilesServiceRequest>,
    ) -> Result<Response<ExportProfilesServiceResponse>, Status> {
        let _ = self.tx.send(ServiceRequest::Profiles(request.into_inner()));
        Ok(Response::new(ExportProfilesServiceResponse::default()))
    }
}
