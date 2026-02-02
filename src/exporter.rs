use std::{future, mem, num::NonZeroUsize, sync::Arc, time::Instant};

use anyhow::Error;
use bytes::BytesMut;
use http_body_util::{BodyExt, Full};
use hyper::{
    Request,
    header::{AUTHORIZATION, CONTENT_TYPE, USER_AGENT},
};
use lru::LruCache;
use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::{ExportLogsServiceRequest, ExportLogsServiceResponse},
        metrics::v1::{ExportMetricsServiceRequest, ExportMetricsServiceResponse},
        profiles::v1development::{ExportProfilesServiceRequest, ExportProfilesServiceResponse},
        trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse},
    },
    common::v1::{KeyValue, any_value::Value},
    resource::v1::Resource,
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
use uuid::Uuid;

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

    instance_id: Uuid,
    attributes: Arc<[KeyValue]>,
    /// Maps FaaS invocation IDs to trace and span IDs
    cache: LruCache<Vec<u8>, (Vec<u8>, Vec<u8>)>,
}

trait OtlpRequest: Message {
    type Response: OtlpResponse;

    fn is_empty(&self) -> bool;
    fn resources_mut(&mut self) -> impl Iterator<Item = &mut Resource>;
}
trait OtlpResponse: Message + Default {
    fn log(&self);
}

impl OtlpRequest for ExportTraceServiceRequest {
    type Response = ExportTraceServiceResponse;

    fn is_empty(&self) -> bool {
        self.resource_spans.is_empty()
    }

    fn resources_mut(&mut self) -> impl Iterator<Item = &mut Resource> {
        self.resource_spans
            .iter_mut()
            .filter_map(|rs| rs.resource.as_mut())
    }
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

    fn is_empty(&self) -> bool {
        self.resource_metrics.is_empty()
    }

    fn resources_mut(&mut self) -> impl Iterator<Item = &mut Resource> {
        self.resource_metrics
            .iter_mut()
            .filter_map(|rm| rm.resource.as_mut())
    }
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

    fn is_empty(&self) -> bool {
        self.resource_logs.is_empty()
    }

    fn resources_mut(&mut self) -> impl Iterator<Item = &mut Resource> {
        self.resource_logs
            .iter_mut()
            .filter_map(|rl| rl.resource.as_mut())
    }
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

    fn is_empty(&self) -> bool {
        self.resource_profiles.is_empty()
    }

    fn resources_mut(&mut self) -> impl Iterator<Item = &mut Resource> {
        self.resource_profiles
            .iter_mut()
            .filter_map(|rp| rp.resource.as_mut())
    }
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
    mut request: R,
    mut client: FollowRedirect<Client>,
    url: String,
    token: String,
    instance_id: Uuid,
    attributes: Arc<[KeyValue]>,
) -> Result<(), Error>
where
    R: OtlpRequest,
{
    if request.is_empty() {
        return Ok(());
    }

    for resource in request.resources_mut() {
        crate::detector::augment(resource, instance_id, &attributes);
    }

    let mut buf = BytesMut::with_capacity(request.encoded_len());
    request.encode(&mut buf)?;

    future::poll_fn(|cx| client.poll_ready(cx)).await?;
    let response = client
        .call(
            Request::builder()
                .method("POST")
                .uri(url)
                .header(CONTENT_TYPE, "application/x-protobuf")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .header(USER_AGENT, Config::USER_AGENT)
                .body(body(Full::new(buf.freeze())))?,
        )
        .await?;

    let (parts, body) = response.into_parts();
    let body = body.collect().await?.to_bytes();

    if let Ok(res) = R::Response::decode(body.as_ref()) {
        res.log();
    } else if let Ok(status) = Status::decode(body.as_ref()) {
        eprintln!("failed to export telemetry: {}", status.message);
    } else {
        eprintln!("invalid response from collector: {:#?}", parts);
    }

    Ok(())
}

fn export(state: &mut State, config: &Config, id: Option<String>) {
    let notifier = state.notifier.clone();
    let mut tasks = JoinSet::new();

    let spans = state
        .traces
        .resource_spans
        .iter()
        .flat_map(|rs| rs.scope_spans.iter())
        .flat_map(|ss| ss.spans.iter());

    for span in spans {
        let id = span
            .attributes
            .iter()
            .find(|kv| kv.key == "faas.invocation_id" || kv.key == "faas.execution");

        if let Some(kv) = id
            && let Some(value) = kv.value.as_ref()
            && let Some(Value::StringValue(id)) = value.value.as_ref()
            && span.trace_id.len() == 16
            && span.span_id.len() == 8
        {
            state.cache.push(
                id.as_bytes().to_vec(),
                (span.trace_id.clone(), span.span_id.clone()),
            );
        }
    }

    let logs = state
        .logs
        .resource_logs
        .iter_mut()
        .flat_map(|rl| rl.scope_logs.iter_mut())
        .flat_map(|sl| sl.log_records.iter_mut());

    for log in logs {
        // This indicates we set the invocation ID as trace ID
        if !log.trace_id.is_empty()
            && log.trace_id.len() != 16
            && let Some((trace_id, span_id)) = state.cache.get(&log.trace_id)
        {
            log.trace_id = trace_id.clone();
            if log.span_id.is_empty() {
                log.span_id = span_id.clone();
            }
        }
    }

    tasks.spawn(send(
        mem::take(&mut state.traces),
        state.client.clone(),
        config.urls.exporters.traces.clone(),
        config.token.clone(),
        state.instance_id,
        state.attributes.clone(),
    ));

    tasks.spawn(send(
        mem::take(&mut state.metrics),
        state.client.clone(),
        config.urls.exporters.metrics.clone(),
        config.token.clone(),
        state.instance_id,
        state.attributes.clone(),
    ));

    tasks.spawn(send(
        mem::take(&mut state.logs),
        state.client.clone(),
        config.urls.exporters.logs.clone(),
        config.token.clone(),
        state.instance_id,
        state.attributes.clone(),
    ));

    for profile in mem::take(&mut state.profiles) {
        tasks.spawn(send(
            profile,
            state.client.clone(),
            config.urls.exporters.profiles.clone(),
            config.token.clone(),
            state.instance_id,
            state.attributes.clone(),
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
    instance_id: Uuid,
    attributes: Arc<[KeyValue]>,
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

        instance_id,
        attributes,
        cache: LruCache::new(if config.managed {
            const { NonZeroUsize::new(64).unwrap() }
        } else {
            const { NonZeroUsize::new(8).unwrap() }
        }),
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
