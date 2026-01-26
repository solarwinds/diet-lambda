use anyhow::Error;
use lambda_extension::{InvokeEvent, NextEvent};
use opentelemetry_proto::tonic::collector::{
    logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
    profiles::v1development::ExportProfilesServiceRequest, trace::v1::ExportTraceServiceRequest,
};
use tokio::{
    signal::unix::SignalKind,
    sync::{mpsc, watch},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use crate::{env::Config, util::flatten};

mod detector;
mod env;
mod exporter;
mod requests;
mod server;
mod settings;
mod util;

pub enum ServiceRequest {
    Trace(ExportTraceServiceRequest),
    Metrics(ExportMetricsServiceRequest),
    Logs(ExportLogsServiceRequest),
    Profiles(ExportProfilesServiceRequest),
    Flush(String),
}

fn main() -> Result<(), Error> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()?;

    let config = Config::parse()?;
    let client = util::client();
    let (tx, rx) = mpsc::unbounded_channel();
    let (notifier, mut watcher) = watch::channel(None);

    let (id, account_id) = rt.block_on(requests::extension::register(&client, &config))?;
    let (service_id, attributes) = detector::detect(account_id);
    let mut init = false;

    let token = CancellationToken::new();
    rt.spawn(signal(token.clone()));

    let result = rt.block_on(async {
        if token
            .run_until_cancelled(requests::telemetry::register(&client, &config, &id))
            .await
            .transpose()?
            .is_none()
        {
            // We have been cancelled
            return Ok(());
        }

        let mut tasks = JoinSet::new();
        tasks.spawn(server::task(tx, token.clone()));
        tasks.spawn(exporter::task(
            rx,
            notifier,
            config.clone(),
            client.clone(),
            token.clone(),
            service_id,
            attributes,
        ));
        tasks.spawn(settings::task(
            config.clone(),
            client.clone(),
            token.clone(),
        ));

        let tasks = rt.spawn({
            let token = token.clone();
            async move {
                while let Some(result) = tasks.join_next().await {
                    if let Err(err) = flatten(result) {
                        token.cancel();
                        eprintln!("{err}");
                    }
                }
            }
        });

        init = true;
        loop {
            let next = token
                .run_until_cancelled(requests::extension::next(&client, &config, &id))
                .await
                .transpose()?;

            match next {
                Some(NextEvent::Invoke(InvokeEvent { request_id, .. })) => {
                    // Wait until the exporter notifies us that the given
                    // request telemetry has been flushed.
                    let done = token
                        .run_until_cancelled(watcher.wait_for(|id| {
                            id.as_ref().is_some_and(|current| current == &request_id)
                        }))
                        .await;

                    // We have been cancelled
                    if done.is_none() {
                        break;
                    }
                }

                Some(NextEvent::Shutdown(..)) | None => {
                    token.cancel();
                    break;
                }
            }
        }

        if let Err(err) = tasks.await {
            eprintln!("{err}");
        }
        Ok(())
    });

    if let Err(err) = result.as_ref() {
        let _ = rt.block_on(requests::extension::error(&client, &config, &id, err, init));
    }
    result
}

/// Waits for a termination signals and then cancels all other tasks.
async fn signal(token: CancellationToken) {
    if let Ok(mut signal) = tokio::signal::unix::signal(SignalKind::terminate())
        && token.run_until_cancelled(signal.recv()).await.is_some()
    {
        token.cancel();
    }
}
