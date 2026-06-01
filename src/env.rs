use std::{
    env,
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Error};
use serde::Deserialize;

#[derive(Deserialize, Default)]
pub struct Env {
    otel_service_name: Option<String>,
    aws_lambda_function_name: Option<String>,

    aws_lambda_initialization_type: Option<String>,
    aws_lambda_runtime_api: Option<String>,
    sw_exporter_compression: Option<String>,

    sw_apm_api_token: Option<String>,
    sw_apm_service_key: Option<String>,

    sw_apm_data_center: Option<String>,
    sw_apm_collector: Option<String>,
    sw_exporter_otlp_endpoint: Option<String>,
    sw_exporter_otlp_traces_endpoint: Option<String>,
    sw_exporter_otlp_metrics_endpoint: Option<String>,
    sw_exporter_otlp_logs_endpoint: Option<String>,
    sw_exporter_otlp_profiles_endpoint: Option<String>,
}

pub struct Config {
    pub _service: String,
    pub token: String,

    pub executable: String,
    pub managed: bool,
    pub compression: Compression,

    pub urls: UrlsConfig,
}

#[derive(Clone, Copy)]
pub enum Compression {
    Gzip,
    Zstd,
}

pub struct UrlsConfig {
    pub settings: String,
    pub exporters: ExportersUrlsConfig,
    pub extension: ExtensionUrlsConfig,
    pub telemetry: TelemetryUrlsConfig,
}

pub struct ExportersUrlsConfig {
    pub traces: String,
    pub metrics: String,
    pub logs: String,
    pub profiles: String,
}

pub struct ExtensionUrlsConfig {
    pub register: String,
    pub event: String,
    pub init_error: String,
    pub exit_error: String,
}

pub struct TelemetryUrlsConfig {
    pub register: String,
    pub endpoint: String,
}

impl Config {
    pub const USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));
    pub const BIND_ADDRESSES: [SocketAddrV4; 2] = [
        SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 4317),
        SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 4318),
    ];
    pub const ID_HEADER: &str = "Lambda-Extension-Identifier";

    pub const SETTINGS_PATH: &str = "/tmp/solarwinds-apm-settings.json";
    pub const SETTINGS_INTERVAL: Duration = Duration::from_secs(10);

    pub const TRACES_ROUTE: &str = "/v1/traces";
    pub const METRICS_ROUTE: &str = "/v1/metrics";
    pub const LOGS_ROUTE: &str = "/v1/logs";
    pub const PROFILES_ROUTE: &str = "/v1development/profiles";
    pub const TELEMETRY_ROUTE: &str = "/2022-07-01/telemetry";

    const EXTENSION_VERSION: &str = "2020-01-01";
    const API_HOST: &str = "localhost:9001";
    const LOCAL_HOST: &str = "sandbox.localdomain";

    pub fn parse() -> Result<Arc<Self>, Error> {
        let Env {
            otel_service_name,
            aws_lambda_function_name,

            aws_lambda_initialization_type,
            aws_lambda_runtime_api,
            sw_exporter_compression,

            sw_apm_api_token,
            sw_apm_service_key,

            sw_apm_data_center,
            sw_apm_collector,
            sw_exporter_otlp_endpoint,
            sw_exporter_otlp_traces_endpoint,
            sw_exporter_otlp_metrics_endpoint,
            sw_exporter_otlp_logs_endpoint,
            sw_exporter_otlp_profiles_endpoint,
        } = envy::from_env().unwrap_or_default();

        let service_key = sw_apm_service_key.as_ref().and_then(|s| s.split_once(':'));

        let service_name = otel_service_name
            .or_else(|| service_key.map(|(name, _)| name.to_string()))
            .or(aws_lambda_function_name)
            .context("missing service name")?;

        let managed =
            aws_lambda_initialization_type.is_some_and(|v| v == "lambda-managed-instances");
        let api_host = aws_lambda_runtime_api.unwrap_or_else(|| Self::API_HOST.to_string());

        let api_token = sw_apm_api_token
            .or_else(|| service_key.map(|(_, token)| token.to_string()))
            .unwrap_or_else(|| {
                eprintln!("Missing SolarWinds APM API token. Please set the `SW_APM_API_TOKEN` environment variable to enable sampling.");
                "missing".to_string()
            });

        let data_center = sw_apm_data_center.unwrap_or_else(|| "na-01".to_string());
        let mut collector = sw_apm_collector
            .unwrap_or_else(|| format!("https://apm.collector.{data_center}.cloud.solarwinds.com"));
        let mut exporter = sw_exporter_otlp_endpoint
            .unwrap_or_else(|| collector.replace("apm.collector", "otel.collector"));

        for url in [&mut collector, &mut exporter] {
            if !url.starts_with("https://") && !url.starts_with("http://") {
                *url = format!("https://{url}");
            }
        }

        let executable = env::current_exe()
            .ok()
            .and_then(|path| {
                path.file_name()
                    .map(|name| name.to_string_lossy().into_owned())
            })
            .unwrap_or_else(|| env!("CARGO_PKG_NAME").to_string());

        let compression = sw_exporter_compression
            .and_then(|c| match c.to_lowercase().trim() {
                "gzip" | "gz" => Some(Compression::Gzip),
                "zstd" => Some(Compression::Zstd),
                _ => None,
            })
            .unwrap_or(Compression::Gzip);

        Ok(Arc::new(Self {
            urls: UrlsConfig {
                settings: format!("{collector}/v1/settings/{service_name}/{service_name}",),
                exporters: ExportersUrlsConfig {
                    traces: sw_exporter_otlp_traces_endpoint
                        .unwrap_or_else(|| format!("{exporter}{}", Self::TRACES_ROUTE)),
                    metrics: sw_exporter_otlp_metrics_endpoint
                        .unwrap_or_else(|| format!("{exporter}{}", Self::METRICS_ROUTE)),
                    logs: sw_exporter_otlp_logs_endpoint
                        .unwrap_or_else(|| format!("{exporter}{}", Self::LOGS_ROUTE)),
                    profiles: sw_exporter_otlp_profiles_endpoint
                        .unwrap_or_else(|| format!("{exporter}{}", Self::PROFILES_ROUTE)),
                },
                extension: ExtensionUrlsConfig {
                    register: format!(
                        "http://{api_host}/{}/extension/register",
                        Self::EXTENSION_VERSION
                    ),
                    event: format!(
                        "http://{api_host}/{}/extension/event/next",
                        Self::EXTENSION_VERSION
                    ),
                    init_error: format!(
                        "http://{api_host}/{}/extension/init/error",
                        Self::EXTENSION_VERSION
                    ),
                    exit_error: format!(
                        "http://{api_host}/{}/extension/exit/error",
                        Self::EXTENSION_VERSION
                    ),
                },
                telemetry: TelemetryUrlsConfig {
                    register: format!("http://{api_host}{}", Self::TELEMETRY_ROUTE),
                    endpoint: format!("http://{}:4318{}", Self::LOCAL_HOST, Self::TELEMETRY_ROUTE),
                },
            },

            _service: service_name,
            token: api_token,

            executable,
            managed,
            compression,
        }))
    }
}
