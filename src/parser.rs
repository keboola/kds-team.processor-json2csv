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

        // Initialize root table with default headers if it doesn't exist
        if !self.tables.contains_key("root") {
            self.tables.insert("root".to_string(), TableData {
                headers: Vec::new(),
                rows: Vec::new(),
            });
        }

        // If root_node is specified, get that node first
        let root_value = if !self.config.parameters.root_node.is_empty() {
            self.get_root_node(&json, &self.config.parameters.root_node)?
        } else {
            &json
        };

        // Handle both array and object inputs
        match root_value {
            Value::Array(arr) => {
                for item in arr {
                    self.process_value(item, "root".to_string(), None, &file_name)?;
                }
            }
            _ => {
                self.process_value(root_value, "root".to_string(), None, &file_name)?;
            }
        }

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

                // Process each field in the object
                for (key, val) in obj {
                    match val {
                        Value::Object(_) => {
                            // Process nested objects recursively
                            self.process_value(val, key.clone(), Some(table_name.clone()), file_name)?;
                        }
                        Value::Array(arr) => {
                            // Process array items as a separate table
                            for (i, item) in arr.iter().enumerate() {
                                let parent_id = format!("{}_{}", key, i);
                                self.process_value(item, key.clone(), Some(parent_id), file_name)?;
                            }
                        }
                        _ => {
                            let value_str = self.format_value(val);
                            row.insert(key.clone(), value_str);
                            
                            // Add header if it doesn't exist
                            let table = self.tables.get_mut(&table_name).unwrap();
                            if !table.headers.contains(key) {
                                table.headers.push(key.clone());
                            }
                        }
                    }
                }

                // Add parent ID if this is a child table
                if let Some(parent_id) = parent_path {
                    row.insert("JSON_parentId".to_string(), parent_id);
                    
                    // Add JSON_parentId header if it doesn't exist
                    let table = self.tables.get_mut(&table_name).unwrap();
                    if !table.headers.contains(&"JSON_parentId".to_string()) {
                        table.headers.push("JSON_parentId".to_string());
                    }
                }

                // Add file name if configured
                if self.config.parameters.add_file_name && table_name == "root" {
                    row.insert("keboola_file_name_col".to_string(), format!("{} ", file_name));
                    
                    // Add keboola_file_name_col header if it doesn't exist
                    let table = self.tables.get_mut(&table_name).unwrap();
                    if !table.headers.contains(&"keboola_file_name_col".to_string()) {
                        table.headers.push("keboola_file_name_col".to_string());
                    }
                }

                // Fill in missing values with empty strings
                let table = self.tables.get_mut(&table_name).unwrap();
                let mut row_with_all_headers = HashMap::new();
                for header in &table.headers {
                    row_with_all_headers.insert(header.clone(), row.get(header).cloned().unwrap_or_default());
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
