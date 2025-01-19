use serde_json::Value;
use anyhow::{Result, anyhow};

pub struct ValueProcessor;

impl ValueProcessor {
    pub fn format_value(value: &Value) -> String {
        match value {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Null => String::new(),
            _ => value.to_string(),
        }
    }

    pub fn get_root_node<'a>(json: &'a Value, root_node: &str) -> Result<&'a Value> {
        if root_node.is_empty() {
            return Ok(json);
        }

        let mut current = json;
        for node in root_node.split('.') {
            current = current.get(node).ok_or_else(|| {
                anyhow!(
                    "Root node path '{}' not found in JSON at node '{}'",
                    root_node,
                    node
                )
            })?;
        }
        Ok(current)
    }
} 