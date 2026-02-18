use std::env;

use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};

pub fn detect(attributes: &mut Vec<KeyValue>, account_id: Option<String>) {
    attributes.push(KeyValue {
        key: "sw.cloud.aws.resource.type".to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue("Lambda".to_string())),
        }),
    });

    attributes.push(KeyValue {
        key: "cloud.provider".to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue("aws".to_string())),
        }),
    });
    attributes.push(KeyValue {
        key: "cloud.platform".to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue("aws_lambda".to_string())),
        }),
    });

    let function_name = env::var("AWS_LAMBDA_FUNCTION_NAME").ok();
    let region = env::var("AWS_REGION")
        .or_else(|_| env::var("AWS_DEFAULT_REGION"))
        .ok();

    if let Some(function_name) = function_name.as_ref() {
        attributes.push(KeyValue {
            key: "faas.name".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(function_name.clone())),
            }),
        });
    }

    if let Some(account_id) = account_id.as_ref() {
        attributes.push(KeyValue {
            key: "cloud.account.id".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(account_id.clone())),
            }),
        });
    }

    if let Some(region) = region.as_ref() {
        attributes.push(KeyValue {
            key: "cloud.region".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(region.clone())),
            }),
        });
    }

    if let Some(function_name) = function_name.as_ref()
        && let Some(account_id) = account_id
        && let Some(region) = region
    {
        attributes.push(KeyValue {
            key: "cloud.resource_id".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(format!(
                    "arn:aws:lambda:{region}:{account_id}:function:{function_name}"
                ))),
            }),
        });
    }

    if let Ok(function_version) = env::var("AWS_LAMBDA_FUNCTION_VERSION") {
        attributes.push(KeyValue {
            key: "faas.version".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(function_version)),
            }),
        });
    }

    if let Some(service_name) = env::var("OTEL_SERVICE_NAME").ok().or(function_name) {
        attributes.push(KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(service_name)),
            }),
        });
    }
}
