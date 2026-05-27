use cfg_if::cfg_if;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};

cfg_if! {
    if #[cfg(target_os = "linux")] {
        const OS: Option<&'static str> = Some("linux");
    } else {
        const OS: Option<&'static str> = None;
    }
}

cfg_if! {
    if #[cfg(target_arch = "x86_64")] {
        const ARCH: Option<&'static str> = Some("amd64");
    } else if #[cfg(target_arch = "aarch64")] {
        const ARCH: Option<&'static str> = Some("arm64");
    } else {
        const ARCH: Option<&'static str> = None;
    }
}

pub fn detect(attributes: &mut Vec<KeyValue>) {
    if let Some(os) = OS {
        attributes.push(KeyValue {
            key: "os.type".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(os.to_string())),
            }),
            ..Default::default()
        });
    }

    if let Some(arch) = ARCH {
        attributes.push(KeyValue {
            key: "host.arch".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(arch.to_string())),
            }),
            ..Default::default()
        });
    }
}
