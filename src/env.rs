use std::{
    env,
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Error};

pub struct Config {
    pub _service: String,
    pub token: String,

    pub executable: String,
    pub managed: bool,

    pub urls: UrlsConfig,
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

    pub const SETTINGS_PATH: &str = "/var/tmp/solarwinds-apm-settings.json";
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
        let service_key = env::var("SW_APM_SERVICE_KEY").ok();
        let service_key = service_key.as_ref().and_then(|s| s.split_once(':'));

        let service_name = env::var("OTEL_SERVICE_NAME")
            .ok()
            .or_else(|| service_key.map(|(name, _)| name.to_string()))
            .or_else(|| env::var("AWS_LAMBDA_FUNCTION_NAME").ok())
            .context("missing service name")?;

        let api_token = env::var("SW_APM_API_TOKEN")
            .ok()
            .or_else(|| service_key.map(|(_, token)| token.to_string()))
            .context("missing API token")?;

        let data_center = env::var("SW_APM_DATA_CENTER")
            .ok()
            .unwrap_or_else(|| "na-01".to_string());

        let api_host =
            env::var("AWS_LAMBDA_RUNTIME_API").unwrap_or_else(|_| Self::API_HOST.to_string());

        let executable = env::current_exe()
            .ok()
            .and_then(|path| {
                path.file_name()
                    .map(|name| name.to_string_lossy().into_owned())
            })
            .unwrap_or_else(|| env!("CARGO_PKG_NAME").to_string());

        let managed = env::var("AWS_LAMBDA_INITIALIZATION_TYPE")
            .is_ok_and(|v| v == "lambda-managed-instances");

        Ok(Arc::new(Self {
            urls: UrlsConfig {
                settings: format!(
                    "https://apm.collector.{data_center}.cloud.solarwinds.com/v1/settings/{service_name}/{service_name}",
                ),
                exporters: ExportersUrlsConfig {
                    traces: format!(
                        "https://otel.collector.{data_center}.cloud.solarwinds.com{}",
                        Self::TRACES_ROUTE
                    ),
                    metrics: format!(
                        "https://otel.collector.{data_center}.cloud.solarwinds.com{}",
                        Self::METRICS_ROUTE
                    ),
                    logs: format!(
                        "https://otel.collector.{data_center}.cloud.solarwinds.com{}",
                        Self::LOGS_ROUTE
                    ),
                    profiles: format!(
                        "https://otel.collector.{data_center}.cloud.solarwinds.com{}",
                        Self::PROFILES_ROUTE
                    ),
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
        }))
    }
}
