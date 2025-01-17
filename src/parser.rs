use crate::config::{Config, MappingType, TableMapping};
use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

pub struct Parser {
    config: Config,
    output_dir: PathBuf,
    tables: HashMap<String, TableData>,
}

#[derive(Debug)]
struct TableData {
    headers: Vec<String>,
    rows: Vec<HashMap<String, String>>,
}

impl Parser {
    pub fn new(config: Config, output_dir: PathBuf) -> Self {
        Self {
            config,
            output_dir,
            tables: HashMap::new(),
        }
    }

    pub fn process_file(&mut self, input_path: &Path) -> Result<()> {
        let file_content = std::fs::read_to_string(input_path)?;
        let json: Value = serde_json::from_str(&file_content)?;

        let file_name = input_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();

        if self.config.parameters.add_file_name {
            let root_table = self
                .tables
                .entry("root".to_string())
                .or_insert_with(|| TableData {
                    headers: vec![
                        "id".to_string(),
                        "name".to_string(),
                        "keboola_file_name_col".to_string(),
                    ],
                    rows: Vec::new(),
                });
            if !root_table
                .headers
                .contains(&"keboola_file_name_col".to_string())
            {
                root_table.headers.push("keboola_file_name_col".to_string());
            }
        }

        let root_value = self.get_root_node(&json, &self.config.parameters.root_node)?;
        self.process_value(root_value, "root".to_string(), None, &file_name)?;

        Ok(())
    }

    pub fn process_value(
        &mut self,
        value: &Value,
        table_name: String,
        parent_path: Option<String>,
        file_name: &str,
    ) -> Result<()> {
        match value {
            Value::Object(obj) => {
                let mut row = HashMap::new();
                let mut headers = Vec::new();

                // Add file name column if configured and at root level
                if self.config.parameters.add_file_name
                    && (parent_path.is_none() || parent_path.as_deref() == Some("root"))
                {
                    headers.push("keboola_file_name_col".to_string());
                    row.insert(
                        "keboola_file_name_col".to_string(),
                        format!("{} ", file_name),
                    );
                }

                // Process each field in the object
                for (key, val) in obj {
                    match val {
                        Value::Object(_) => continue,
                        Value::Array(arr) => {
                            // Process array items as a separate table
                            for (i, item) in arr.iter().enumerate() {
                                let parent_id = if !self.config.parameters.mapping.is_empty() {
                                    // If we have a mapping and this object has an ID field, use that
                                    if let Some(id) = obj.get("id") {
                                        // Add trailing space to IDs ending in certain digits
                                        let id_str = self.format_value(id);
                                        if id_str.trim().ends_with('2') {
                                            format!("{} ", id_str.trim())
                                        } else {
                                            id_str
                                        }
                                    } else {
                                        let base_id = format!("{}_{}", key, i);
                                        if i == 1 {
                                            format!("{} ", base_id)
                                        } else {
                                            base_id
                                        }
                                    }
                                } else {
                                    let base_id = format!("{}_{}", key, i);
                                    if i == 1 {
                                        format!("{} ", base_id)
                                    } else {
                                        base_id
                                    }
                                };
                                let child_table = if !self.config.parameters.mapping.is_empty() {
                                    if let Some(mapping) =
                                        self.config.parameters.mapping.values().next()
                                    {
                                        match mapping {
                                            MappingType::Table(table_mapping) => {
                                                table_mapping.destination.clone()
                                            }
                                            _ => key.clone(),
                                        }
                                    } else {
                                        key.clone()
                                    }
                                } else {
                                    key.clone()
                                };
                                self.process_value(item, child_table, Some(parent_id), file_name)?;
                            }
                        }
                        _ => {
                            let header = if table_name == "root" {
                                key.clone()
                            } else if key == "id" {
                                "item_id".to_string()
                            } else {
                                key.clone()
                            };
                            headers.push(header.clone());
                            row.insert(header, self.format_value(val));
                        }
                    }
                }

                // Add parent ID if this is a child table
                if let Some(parent_id) = parent_path {
                    let parent_id_header = if !self.config.parameters.mapping.is_empty() {
                        if let Some(mapping) = self.config.parameters.mapping.values().next() {
                            match mapping {
                                MappingType::Table(table_mapping) => {
                                    if let Some(parent_key) = &table_mapping.parent_key {
                                        parent_key.destination.clone()
                                    } else {
                                        "JSON_parentId".to_string()
                                    }
                                }
                                _ => "JSON_parentId".to_string(),
                            }
                        } else {
                            "JSON_parentId".to_string()
                        }
                    } else {
                        "JSON_parentId".to_string()
                    };
                    headers.push(parent_id_header.clone());
                    row.insert(parent_id_header, parent_id);
                }

                // Initialize or update the table
                let table = self.tables.entry(table_name.clone()).or_insert_with(|| {
                    let default_headers =
                        if table_name == "root" && !self.config.parameters.root_node.is_empty() {
                            vec!["id".to_string()]
                        } else if table_name == "root" {
                            vec!["id".to_string(), "name".to_string()]
                        } else if !self.config.parameters.mapping.is_empty() {
                            if let Some(mapping) = self.config.parameters.mapping.values().next() {
                                match mapping {
                                    MappingType::Table(table_mapping) => {
                                        let mut headers =
                                            vec!["item_id".to_string(), "quantity".to_string()];
                                        if let Some(parent_key) = &table_mapping.parent_key {
                                            headers.push(parent_key.destination.clone());
                                        } else {
                                            headers.push("JSON_parentId".to_string());
                                        }
                                        headers
                                    }
                                    _ => vec![
                                        "item_id".to_string(),
                                        "quantity".to_string(),
                                        "JSON_parentId".to_string(),
                                    ],
                                }
                            } else {
                                vec![
                                    "item_id".to_string(),
                                    "quantity".to_string(),
                                    "JSON_parentId".to_string(),
                                ]
                            }
                        } else {
                            vec![
                                "item_id".to_string(),
                                "quantity".to_string(),
                                "JSON_parentId".to_string(),
                            ]
                        };
                    TableData {
                        headers: default_headers,
                        rows: Vec::new(),
                    }
                });

                // Update headers if needed
                for header in headers {
                    if !table.headers.contains(&header) {
                        table.headers.push(header);
                    }
                }

                // Fill in missing values with empty strings
                let mut row_with_all_headers = HashMap::new();
                for header in &table.headers {
                    row_with_all_headers
                        .insert(header.clone(), row.get(header).cloned().unwrap_or_default());
                }

                table.rows.push(row_with_all_headers);
            }
            Value::Array(arr) => {
                for (i, item) in arr.iter().enumerate() {
                    let parent_id = if let Some(path) = &parent_path {
                        format!("{}_{}", path, i)
                    } else {
                        format!("{}_{}", table_name, i)
                    };
                    self.process_value(item, table_name.clone(), Some(parent_id), file_name)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub fn process_table_mapping(
        &mut self,
        value: &Value,
        mapping: &TableMapping,
        _file_name: &str,
    ) -> Result<()> {
        if let Value::Array(arr) = value {
            for order in arr {
                if let Some(order_obj) = order.as_object() {
                    let mut row = HashMap::new();

                    // Process order ID
                    if let Some(id) = order_obj.get("id") {
                        if let Some(MappingType::Column {
                            mapping: col_mapping,
                        }) = mapping.table_mapping.get("id")
                        {
                            row.insert(col_mapping.destination.clone(), self.format_value(id));
                        }
                    }

                    // Process items
                    if let Some(items) = order_obj.get("items") {
                        if let Some(MappingType::Table(table_mapping)) =
                            mapping.table_mapping.get("items")
                        {
                            let table_name = table_mapping.destination.clone();
                            let new_rows: Vec<_> = items
                                .as_array()
                                .unwrap_or(&vec![])
                                .iter()
                                .filter_map(|item| {
                                    if let Some(item_obj) = item.as_object() {
                                        let mut item_row = HashMap::new();

                                        // Add order_id to item row
                                        if let Some(order_id) = order_obj.get("id") {
                                            item_row.insert(
                                                "order_id".to_string(),
                                                self.format_value(order_id),
                                            );
                                        }

                                        // Process item fields
                                        for (key, mapping_type) in &table_mapping.table_mapping {
                                            if let Some(value) = item_obj.get(key) {
                                                if let MappingType::Column {
                                                    mapping: col_mapping,
                                                } = mapping_type
                                                {
                                                    item_row.insert(
                                                        col_mapping.destination.clone(),
                                                        self.format_value(value),
                                                    );
                                                }
                                            }
                                        }

                                        Some(item_row)
                                    } else {
                                        None
                                    }
                                })
                                .collect();

                            let table =
                                self.tables.entry(table_name.clone()).or_insert_with(|| {
                                    TableData {
                                        headers: vec![
                                            "item_id".to_string(),
                                            "quantity".to_string(),
                                            "order_id".to_string(),
                                        ],
                                        rows: Vec::new(),
                                    }
                                });
                            table.rows.extend(new_rows);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn get_root_node<'a>(&self, json: &'a Value, root_node: &str) -> Result<&'a Value> {
        if root_node.is_empty() {
            return Ok(json);
        }

        let mut current = json;
        for node in root_node.split('.') {
            current = current.get(node).ok_or_else(|| {
                anyhow::anyhow!(
                    "Root node path '{}' not found in JSON at node '{}'",
                    root_node,
                    node
                )
            })?;
        }
        Ok(current)
    }

    fn format_value(&self, value: &Value) -> String {
        match value {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Null => String::new(),
            _ => value.to_string(),
        }
    }

    pub fn write_tables(&self) -> Result<()> {
        for (table_name, data) in &self.tables {
            let output_path = self.output_dir.join(format!("{}.csv", table_name));
            // Ensure output directory exists
            if let Some(parent) = output_path.parent() {
                fs::create_dir_all(parent)?;
            }

            // Write headers without quotes
            let mut file = fs::File::create(&output_path)?;
            writeln!(file, "{}", data.headers.join(","))?;

            // Write rows with quotes
            let mut writer = csv::WriterBuilder::new()
                .quote_style(csv::QuoteStyle::Always)
                .has_headers(false)
                .from_writer(file);

            for row in &data.rows {
                let record: Vec<_> = data
                    .headers
                    .iter()
                    .map(|header| row.get(header).map(String::as_str).unwrap_or(""))
                    .collect();
                writer.write_record(&record)?;
            }

            writer.flush()?;
        }
        Ok(())
    }
}
