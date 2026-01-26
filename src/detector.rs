use std::{collections::HashMap, sync::Arc};

use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, KeyValue, any_value::Value},
    resource::v1::Resource,
};
use uuid::Uuid;

mod host;
mod lambda;
mod sdk;

pub fn detect(account_id: Option<String>) -> (Uuid, Arc<[KeyValue]>) {
    let mut attributes = Vec::new();

    sdk::detect(&mut attributes);
    host::detect(&mut attributes);
    lambda::detect(&mut attributes, account_id);

    (Uuid::new_v4(), attributes.into())
}

pub fn augment(resource: &mut Resource, service_id: Uuid, attributes: &[KeyValue]) {
    let positions: HashMap<String, usize> = resource
        .attributes
        .iter()
        .enumerate()
        .map(|(i, kv)| (kv.key.clone(), i))
        .collect();

    // Override instance ID if it already exists
    match positions.get("service.instance.id") {
        Some(&index) => {
            resource.attributes[index].value = Some(AnyValue {
                value: Some(Value::StringValue(service_id.to_string())),
            });
        }
        None => {
            resource.attributes.push(KeyValue {
                key: "service.instance.id".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(service_id.to_string())),
                }),
            });
        }
    }

    for attr in attributes {
        if !positions.contains_key(&attr.key) {
            resource.attributes.push(attr.clone());
        }
    }
}
