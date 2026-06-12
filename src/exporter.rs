use std::{
    fmt::Debug, future, io::Cursor, mem, num::NonZeroUsize, pin::pin, sync::Arc, task::Poll,
    time::Duration,
};

use anyhow::Error;
use async_compression::{
    Level,
    tokio::bufread::{GzipEncoder, ZstdEncoder},
};
use bytes::BytesMut;
use futures_util::TryStreamExt;
use http_body_util::{BodyExt, StreamBody};
use hyper::{
    Request,
    body::Frame,
    header::{AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE, USER_AGENT},
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
    io::AsyncRead,
    sync::{mpsc, watch},
    task::JoinSet,
    time::{self, MissedTickBehavior},
};
use tokio_util::{io::ReaderStream, sync::CancellationToken, task::TaskTracker};
use tower::Service;
use tower_http::follow_redirect::FollowRedirect;
use uuid::Uuid;

use crate::{
    ServiceRequest,
    env::{Compression, Config},
    util::{Client, body, flatten},
};

const INTERVAL: Duration = Duration::from_secs(60);
const MAX_BUFFERED: usize = 8 * 1024 * 1024; // 8 MiB

struct State {
    client: FollowRedirect<Client>,
    buffered: usize,

    traces: ExportTraceServiceRequest,
    metrics: ExportMetricsServiceRequest,
    logs: ExportLogsServiceRequest,
    profiles: Vec<ExportProfilesServiceRequest>,

    notifier: watch::Sender<Option<String>>,
    tracker: TaskTracker,

    instance_id: Uuid,
    attributes: Arc<[KeyValue]>,
    /// Maps FaaS invocation IDs -> trace+span IDs
    cache: LruCache<Vec<u8>, (Vec<u8>, Vec<u8>)>,
}

trait OtlpRequest: Message + Debug {
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
            tracing::warn!(
                err = ?partial,
                "failed to export traces",
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
            tracing::warn!(
                err = ?partial,
                "failed to export metrics",
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
            tracing::warn!(
                err = ?partial,
                "failed to export logs",
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
            tracing::warn!(
                err = ?partial,
                "failed to export profiles",
            );
        }
    }
}

async fn send<R>(
    mut request: R,
    mut client: FollowRedirect<Client>,
    url: String,
    token: String,
    compression: Compression,
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

    tracing::trace!(req = ?request, "exporting telemetry");
    let mut buf = BytesMut::with_capacity(request.encoded_len());
    request.encode(&mut buf)?;

    let (boxed, encoding): (Box<dyn AsyncRead + Unpin + Send>, &str) = match compression {
        Compression::Zstd => (
            Box::new(ZstdEncoder::with_quality(
                Cursor::new(buf),
                Level::Precise(4),
            )),
            "zstd",
        ),
        Compression::Gzip => (
            Box::new(GzipEncoder::with_quality(
                Cursor::new(buf),
                Level::Precise(6),
            )),
            "gzip",
        ),
    };
    let compressed = StreamBody::new(ReaderStream::new(boxed).map_ok(Frame::data));

    future::poll_fn(|cx| client.poll_ready(cx)).await?;
    let response = client
        .call(
            Request::builder()
                .method("POST")
                .uri(url)
                .header(CONTENT_TYPE, "application/x-protobuf")
                .header(CONTENT_ENCODING, encoding)
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .header(USER_AGENT, Config::USER_AGENT)
                .body(body(compressed))?,
        )
        .await?;

    let (parts, body) = response.into_parts();
    let body = body.collect().await?.to_bytes();

    if let Ok(res) = R::Response::decode(body.as_ref()) {
        res.log();
    } else if let Ok(status) = Status::decode(body.as_ref()) {
        tracing::warn!(status = status.message, "failed to export telemetry");
    } else {
        tracing::warn!(res = ?parts, "invalid response from collector");
    }

    Ok(())
}

#[tracing::instrument(level = "debug", skip_all)]
fn export(state: &mut State, config: &Config, id: Option<String>) {
    let notifier = state.notifier.clone();
    let mut tasks = JoinSet::new();

    let spans = state
        .traces
        .resource_spans
        .iter()
        .flat_map(|rs| rs.scope_spans.iter())
        .flat_map(|ss| ss.spans.iter());

    // populate the cache with AWS request ID -> trace+span ID mappings
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
        if !log.trace_id.is_empty() && log.trace_id.len() != 16 {
            // This indicates we set the request ID as trace ID
            if let Some((trace_id, span_id)) = state.cache.get(&log.trace_id) {
                log.trace_id = trace_id.clone();
                if log.span_id.is_empty() {
                    log.span_id = span_id.clone();
                }
            } else {
                // Don't leave an invalid trace ID in for logs we fail to correlate
                log.trace_id = Vec::new();
            }
        }
    }

    tasks.spawn(send(
        mem::take(&mut state.traces),
        state.client.clone(),
        config.urls.exporters.traces.clone(),
        config.token.clone(),
        config.compression,
        state.instance_id,
        state.attributes.clone(),
    ));

    tasks.spawn(send(
        mem::take(&mut state.metrics),
        state.client.clone(),
        config.urls.exporters.metrics.clone(),
        config.token.clone(),
        config.compression,
        state.instance_id,
        state.attributes.clone(),
    ));

    tasks.spawn(send(
        mem::take(&mut state.logs),
        state.client.clone(),
        config.urls.exporters.logs.clone(),
        config.token.clone(),
        config.compression,
        state.instance_id,
        state.attributes.clone(),
    ));

    for profile in mem::take(&mut state.profiles) {
        tasks.spawn(send(
            profile,
            state.client.clone(),
            config.urls.exporters.profiles.clone(),
            config.token.clone(),
            config.compression,
            state.instance_id,
            state.attributes.clone(),
        ));
    }

    state.tracker.spawn(async move {
        while let Some(result) = tasks.join_next().await {
            if let Err(err) = flatten(result) {
                tracing::warn!(%err, "failed to export telemetry");
            }
        }
        let _ = notifier.send(id);
    });

    state.buffered = 0;
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

        traces: ExportTraceServiceRequest::default(),
        metrics: ExportMetricsServiceRequest::default(),
        logs: ExportLogsServiceRequest::default(),
        profiles: Vec::default(),

        tracker: TaskTracker::new(),
        notifier,

        instance_id,
        attributes,
        cache: LruCache::new(if config.managed {
            const { NonZeroUsize::new(1024).unwrap() }
        } else {
            const { NonZeroUsize::new(8).unwrap() }
        }),
    };

    loop {
        let mut interval = time::interval(INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut message = pin!(token.run_until_cancelled(rx.recv()));
        let next = future::poll_fn(|cx| {
            if let Poll::Ready(message) = message.as_mut().poll(cx) {
                Poll::Ready(message.flatten())
            } else if interval.poll_tick(cx).is_ready() && config.managed {
                // Flush periodically for managed runtimes. We can set an empty
                // request ID since nothing is waiting for flush notifications.
                Poll::Ready(Some(ServiceRequest::Flush(String::new())))
            } else {
                Poll::Pending
            }
        })
        .await;

        match next {
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
                continue;
            }
            // We're shutting down
            None => {
                export(&mut state, &config, None);
                break;
            }
        }

        if state.buffered >= MAX_BUFFERED {
            export(&mut state, &config, None);
        }
    }

    state.tracker.close();
    state.tracker.wait().await;

    Ok(())
}
