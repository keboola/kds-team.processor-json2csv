use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Invalid input type: {0}")]
    InvalidInputType(String),
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum InputType {
    Files,
    Tables,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ColumnMapping {
    pub destination: String,
    #[serde(default)]
    pub primary_key: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TableMapping {
    pub destination: String,
    pub parent_key: Option<ColumnMapping>,
    #[serde(rename = "tableMapping")]
    pub table_mapping: HashMap<String, MappingType>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
pub enum MappingType {
    #[serde(rename = "column")]
    Column { mapping: ColumnMapping },
    #[serde(rename = "table")]
    Table(TableMapping),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub parameters: Parameters,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Parameters {
    #[serde(default)]
    pub mapping: HashMap<String, MappingType>,
    #[serde(default)]
    pub incremental: bool,
    #[serde(default)]
    pub root_node: String,
    pub in_type: InputType,
    #[serde(default)]
    pub add_file_name: bool,
}

impl Config {
    pub fn validate(&self) -> Result<(), ConfigError> {
        // Add validation logic here if needed
        Ok(())
    }
}
