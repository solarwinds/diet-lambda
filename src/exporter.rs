use std::{future, mem, sync::Arc, time::Instant};

use anyhow::Error;
use bytes::BytesMut;
use http_body_util::{BodyExt, Full};
use hyper::{
    Request,
    header::{AUTHORIZATION, CONTENT_TYPE, USER_AGENT},
};
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
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tower::Service;
use tower_http::follow_redirect::FollowRedirect;

use crate::{
    ServiceRequest,
    env::Config,
    util::{Client, body, flatten},
};

struct State {
    client: FollowRedirect<Client>,
    buffered: usize,
    flushed: Instant,

    traces: ExportTraceServiceRequest,
    metrics: ExportMetricsServiceRequest,
    logs: ExportLogsServiceRequest,
    profiles: Vec<ExportProfilesServiceRequest>,

    notifier: watch::Sender<Option<String>>,
    tracker: TaskTracker,
}

trait OtlpRequest: Message {
    type Response: OtlpResponse;
}
trait OtlpResponse: Message + Default {
    fn log(&self);
}

impl OtlpRequest for ExportTraceServiceRequest {
    type Response = ExportTraceServiceResponse;
}
impl OtlpResponse for ExportTraceServiceResponse {
    fn log(&self) {
        if let Some(partial) = &self.partial_success
            && (partial.rejected_spans > 0 || !partial.error_message.is_empty())
        {
            eprintln!(
                "failed to export {n} traces: {message}",
                n = partial.rejected_spans,
                message = partial.error_message,
            );
        }
    }
}

impl OtlpRequest for ExportMetricsServiceRequest {
    type Response = ExportMetricsServiceResponse;
}
impl OtlpResponse for ExportMetricsServiceResponse {
    fn log(&self) {
        if let Some(partial) = &self.partial_success
            && (partial.rejected_data_points > 0 || !partial.error_message.is_empty())
        {
            eprintln!(
                "failed to export {n} metrics: {message}",
                n = partial.rejected_data_points,
                message = partial.error_message,
            );
        }
    }
}

impl OtlpRequest for ExportLogsServiceRequest {
    type Response = ExportLogsServiceResponse;
}
impl OtlpResponse for ExportLogsServiceResponse {
    fn log(&self) {
        if let Some(partial) = &self.partial_success
            && (partial.rejected_log_records > 0 || !partial.error_message.is_empty())
        {
            eprintln!(
                "failed to export {n} logs: {message}",
                n = partial.rejected_log_records,
                message = partial.error_message,
            );
        }
    }
}

impl OtlpRequest for ExportProfilesServiceRequest {
    type Response = ExportProfilesServiceResponse;
}
impl OtlpResponse for ExportProfilesServiceResponse {
    fn log(&self) {
        if let Some(partial) = &self.partial_success
            && (partial.rejected_profiles > 0 || !partial.error_message.is_empty())
        {
            eprintln!(
                "failed to export {n} profiles: {message}",
                n = partial.rejected_profiles,
                message = partial.error_message,
            );
        }
    }
}

async fn send<R>(
    request: R,
    mut client: FollowRedirect<Client>,
    url: String,
    token: String,
) -> Result<(), Error>
where
    R: OtlpRequest,
{
    let mut buf = BytesMut::with_capacity(request.encoded_len());
    request.encode(&mut buf)?;

    future::poll_fn(|cx| client.poll_ready(cx)).await?;
    let response = client
        .call(
            Request::builder()
                .uri(url)
                .header(CONTENT_TYPE, "application/x-protobuf")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .header(USER_AGENT, Config::USER_AGENT)
                .body(body(Full::new(buf.freeze())))?,
        )
        .await?;

    let body = response.into_body().collect().await?.to_bytes();
    if let Ok(res) = R::Response::decode(body.as_ref()) {
        res.log();
    } else if let Ok(status) = Status::decode(body.as_ref()) {
        eprintln!("failed to export telemetry: {}", status.message);
    } else {
        eprintln!("invalid response from collector");
    }

    Ok(())
}

fn export(state: &mut State, config: &Config, id: Option<String>) {
    let notifier = state.notifier.clone();
    let mut tasks = JoinSet::new();

    tasks.spawn(send(
        mem::take(&mut state.traces),
        state.client.clone(),
        config.urls.exporters.traces.clone(),
        config.token.clone(),
    ));

    tasks.spawn(send(
        mem::take(&mut state.metrics),
        state.client.clone(),
        config.urls.exporters.metrics.clone(),
        config.token.clone(),
    ));

    tasks.spawn(send(
        mem::take(&mut state.logs),
        state.client.clone(),
        config.urls.exporters.logs.clone(),
        config.token.clone(),
    ));

    for profile in mem::take(&mut state.profiles) {
        tasks.spawn(send(
            profile,
            state.client.clone(),
            config.urls.exporters.profiles.clone(),
            config.token.clone(),
        ));
    }

    state.tracker.spawn(async move {
        while let Some(result) = tasks.join_next().await {
            if let Err(err) = flatten(result) {
                eprintln!("failed to export telemetry: {err}");
            }
        }
        let _ = notifier.send(id);
    });

    state.buffered = 0;
    state.flushed = Instant::now();
}

pub async fn task(
    mut rx: mpsc::UnboundedReceiver<ServiceRequest>,
    notifier: watch::Sender<Option<String>>,
    config: Arc<Config>,
    client: Client,
    token: CancellationToken,
) -> Result<(), Error> {
    let mut state = State {
        client: FollowRedirect::new(client),
        buffered: 0,
        flushed: Instant::now(),

        traces: ExportTraceServiceRequest::default(),
        metrics: ExportMetricsServiceRequest::default(),
        logs: ExportLogsServiceRequest::default(),
        profiles: Vec::default(),

        tracker: TaskTracker::new(),
        notifier,
    };

    loop {
        let message = token.run_until_cancelled(rx.recv()).await.flatten();

        match message {
            Some(ServiceRequest::Trace(request)) => {
                state.buffered += request.encoded_len();
                state.traces.resource_spans.extend(request.resource_spans);
            }
            Some(ServiceRequest::Metrics(request)) => {
                state.buffered += request.encoded_len();
                state
                    .metrics
                    .resource_metrics
                    .extend(request.resource_metrics);
            }
            Some(ServiceRequest::Logs(request)) => {
                state.buffered += request.encoded_len();
                state.logs.resource_logs.extend(request.resource_logs)
            }
            Some(ServiceRequest::Profiles(request)) => {
                state.buffered += request.encoded_len();
                state.profiles.push(request);
            }

            Some(ServiceRequest::Flush(id)) => {
                export(&mut state, &config, Some(id));
            }
            // We're shutting down
            None => {
                export(&mut state, &config, None);
                break;
            }
        }

        if state.flushed.elapsed().as_secs() > 60 || state.buffered >= 8 * 1024 * 1024 {
            export(&mut state, &config, None);
        }
    }

    state.tracker.close();
    state.tracker.wait().await;

    Ok(())
}
