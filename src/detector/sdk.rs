use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};

pub fn detect(attributes: &mut Vec<KeyValue>) {
    attributes.push(KeyValue {
        key: "sw.data.module".to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue("apm".to_string())),
        }),
        ..Default::default()
    });
    attributes.push(KeyValue {
        key: "sw.apm.otelcol.version".to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(
                concat!(env!("CARGO_PKG_VERSION"), "+rust").to_string(),
            )),
        }),
        ..Default::default()
    });

    attributes.push(KeyValue {
        key: "telemetry.sdk.language".to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue("rust".to_string())),
        }),
        ..Default::default()
    });

    attributes.push(KeyValue {
        key: "telemetry.sdk.name".to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(env!("CARGO_PKG_NAME").to_string())),
        }),
        ..Default::default()
    });

    attributes.push(KeyValue {
        key: "telemetry.sdk.version".to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(env!("CARGO_PKG_VERSION").to_string())),
        }),
        ..Default::default()
    });
}
