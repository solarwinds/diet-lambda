use chrono::{DateTime, FixedOffset, Utc};
use const_hex::FromHex;
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, ArrayValue, KeyValue, KeyValueList, any_value},
    logs::v1::LogRecord,
};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value};

pub fn parse(record: String, observed: DateTime<Utc>) -> Option<LogRecord> {
    eprintln!("{record}");
    let observed_time: u64 = observed.timestamp_nanos_opt()?.try_into().ok()?;

    if let Ok(record) = serde_json::from_str::<JsonLogRecord>(&record) {
        convert_json(record, observed_time, None, None, None)
    } else if let Some((time, request_id, severity_text, message)) = parse_text(&record) {
        if let Ok(record) = serde_json::from_str::<JsonLogRecord>(message) {
            convert_json(
                record,
                observed_time,
                Some(time),
                Some(request_id),
                Some(severity_text),
            )
        } else {
            Some(convert_text(
                message.to_string(),
                observed_time,
                Some(time),
                Some(request_id),
                Some(severity_text),
            ))
        }
    } else {
        Some(convert_text(record, observed_time, None, None, None))
    }
}

fn parse_text(record: &str) -> Option<(u64, &str, &str, &str)> {
    static DELIMITERS: &[char] = &[' ', '\t'];

    let (timestamp, rest) = record.split_once(DELIMITERS)?;
    eprintln!("timestamp: {}", timestamp);
    let time = DateTime::parse_from_rfc3339(timestamp)
        .ok()?
        .timestamp_nanos_opt()?
        .try_into()
        .ok()?;
    eprintln!("time: {}", time);

    let (request_id, rest) = rest.split_once(DELIMITERS)?;
    eprintln!("request_id: {}", request_id);
    let (severity_text, message) = rest.split_once(DELIMITERS)?;
    eprintln!("severity_text: {}", severity_text);
    eprintln!("message: {}", message);

    Some((time, request_id, severity_text, message))
}

fn convert_json(
    record: JsonLogRecord,
    observed_time: u64,
    time: Option<u64>,
    request_id: Option<&str>,
    severity_text: Option<&str>,
) -> Option<LogRecord> {
    let time = record
        .timestamp
        .and_then(|ts| ts.timestamp_nanos_opt())
        .and_then(|ts| ts.try_into().ok())
        .or(time)
        .unwrap_or(observed_time);

    let severity_text = record
        .level
        .or_else(|| severity_text.map(|s| s.to_string()))
        .unwrap_or_default();
    let severity_number = severity_number(&severity_text).unwrap_or(0);

    let trace_id = record
        .trace_id
        .or_else(|| record.request_id.map(|id| id.into_bytes()))
        .or_else(|| request_id.map(|id| id.as_bytes().to_vec()))
        .unwrap_or_default();

    let (body, attributes) = match record.message {
        Some(value) => {
            let message = convert_any_value(value, &mut true);

            let mut complex = false;
            let attributes = convert_key_values(record.rest, &mut complex);

            if complex {
                let body = AnyValue {
                    value: Some(any_value::Value::KvlistValue(KeyValueList {
                        values: {
                            let mut values = Vec::with_capacity(attributes.len() + 1);
                            values.push(KeyValue {
                                key: "message".to_string(),
                                value: Some(message),
                            });
                            values.extend(attributes);
                            values
                        },
                    })),
                };

                (body, Vec::new())
            } else {
                (message, attributes)
            }
        }
        None => {
            let body = AnyValue {
                value: Some(any_value::Value::KvlistValue(KeyValueList {
                    values: convert_key_values(record.rest, &mut false),
                })),
            };

            (body, Vec::new())
        }
    };

    Some(LogRecord {
        time_unix_nano: time,
        observed_time_unix_nano: observed_time,
        severity_text,
        severity_number,
        body: Some(body),
        trace_id,
        span_id: record.span_id.unwrap_or_default(),
        attributes,

        ..Default::default()
    })
}

fn convert_text(
    record: String,
    observed_time: u64,
    time: Option<u64>,
    request_id: Option<&str>,
    severity_text: Option<&str>,
) -> LogRecord {
    let time = time.unwrap_or(observed_time);
    let severity_text = severity_text.unwrap_or_default().to_string();
    let severity_number = severity_number(severity_text.as_str()).unwrap_or(0);

    let trace_id = request_id
        .map(|id| id.as_bytes().to_vec())
        .unwrap_or_default();

    LogRecord {
        time_unix_nano: time,
        observed_time_unix_nano: observed_time,
        severity_text,
        severity_number,
        body: Some(AnyValue {
            value: Some(any_value::Value::StringValue(record)),
        }),
        trace_id,

        ..Default::default()
    }
}

fn severity_number(level: &str) -> Option<i32> {
    match level.to_uppercase().as_str() {
        "TRACE" => Some(1),
        "DEBUG" => Some(5),
        "INFO" => Some(9),
        "WARN" | "WARNING" => Some(13),
        "ERROR" => Some(17),
        "FATAL" => Some(21),
        _ => None,
    }
}

fn convert_any_value(json: Value, complex: &mut bool) -> AnyValue {
    match json {
        Value::Null => AnyValue { value: None },
        Value::Bool(value) => AnyValue {
            value: Some(any_value::Value::BoolValue(value)),
        },
        Value::Number(value) => {
            if let Some(i) = value.as_i64() {
                AnyValue {
                    value: Some(any_value::Value::IntValue(i)),
                }
            } else if let Some(f) = value.as_f64() {
                AnyValue {
                    value: Some(any_value::Value::DoubleValue(f)),
                }
            } else {
                AnyValue { value: None }
            }
        }
        Value::String(value) => AnyValue {
            value: Some(any_value::Value::StringValue(value)),
        },
        Value::Array(values) => {
            *complex = true;
            let values = values
                .into_iter()
                .map(|value| convert_any_value(value, complex))
                .collect();
            AnyValue {
                value: Some(any_value::Value::ArrayValue(ArrayValue { values })),
            }
        }
        Value::Object(values) => {
            *complex = true;
            let values = convert_key_values(values, complex);
            AnyValue {
                value: Some(any_value::Value::KvlistValue(KeyValueList { values })),
            }
        }
    }
}

fn convert_key_values(json: Map<String, Value>, complex: &mut bool) -> Vec<KeyValue> {
    json.into_iter()
        .map(|(k, v)| KeyValue {
            key: k,
            value: Some(convert_any_value(v, complex)),
        })
        .collect()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonLogRecord {
    #[serde(alias = "time", default)]
    timestamp: Option<DateTime<FixedOffset>>,
    #[serde(alias = "AWSRequestID", default)]
    request_id: Option<String>,
    #[serde(default)]
    level: Option<String>,

    #[serde(default, deserialize_with = "deserialize_hex_id")]
    trace_id: Option<Vec<u8>>,
    #[serde(default, deserialize_with = "deserialize_hex_id")]
    span_id: Option<Vec<u8>>,

    #[serde(alias = "msg", default)]
    message: Option<Value>,
    #[serde(flatten, default)]
    rest: Map<String, Value>,
}

fn deserialize_hex_id<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<&'de str>::deserialize(deserializer)?
        .map(|hex| Vec::from_hex(hex).map_err(serde::de::Error::custom))
        .transpose()
}
