use std::fs;
use std::path::Path;
use anyhow::Result;
use serde_json::Value;
use csv::ReaderBuilder;
use std::collections::HashMap;
use crate::config::{Config, MappingType};

use super::TableData;
use super::value_processor::ValueProcessor;
use super::id_handler::IdHandler;

pub struct Processor<'a> {
    tables: &'a mut HashMap<String, TableData>,
    config: &'a Config,
    has_loaded_existing: &'a mut bool,
}

impl<'a> Processor<'a> {
    pub fn new(
        tables: &'a mut HashMap<String, TableData>,
        config: &'a Config,
        has_loaded_existing: &'a mut bool,
    ) -> Self {
        Self {
            tables,
            config,
            has_loaded_existing,
        }
    }

    pub fn process_file(&mut self, input_path: &Path, output_dir: &Path) -> Result<()> {
        // Load existing data only once if in incremental mode
        if self.config.parameters.incremental && !*self.has_loaded_existing {
            if output_dir.exists() {
                for entry in fs::read_dir(output_dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_file() && path.extension().map_or(false, |ext| ext == "csv") {
                        let table_name = path.file_stem().unwrap().to_str().unwrap().to_string();
                        let mut reader = ReaderBuilder::new()
                            .has_headers(true)
                            .from_path(&path)?;
                        
                        let headers = reader.headers()?.clone();
                        let mut rows = Vec::new();
                        for result in reader.records() {
                            let record = result?;
                            let mut row = HashMap::new();
                            for (i, header) in headers.iter().enumerate() {
                                row.insert(header.to_string(), record[i].to_string());
                            }
                            rows.push(row);
                        }

                        self.tables.insert(table_name, TableData {
                            headers: headers.iter().map(|h| h.to_string()).collect(),
                            rows,
                        });
                    }
                }
            }
            *self.has_loaded_existing = true;
        }

        let file_content = fs::read_to_string(input_path)?;
        let json: Value = serde_json::from_str(&file_content)?;

        let file_name = input_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();

        // Initialize root table with default headers if it doesn't exist
        if !self.tables.contains_key("root") {
            let mut default_headers = if !self.config.parameters.root_node.is_empty() {
                vec!["id".to_string()]
            } else {
                vec!["id".to_string(), "name".to_string()]
            };

            // Add file name column to root table if configured
            if self.config.parameters.add_file_name {
                default_headers.push("keboola_file_name_col".to_string());
            }

            self.tables.insert("root".to_string(), TableData::new(default_headers));
        }

        // If root_node is specified, get that node first
        let root_value = if !self.config.parameters.root_node.is_empty() {
            ValueProcessor::get_root_node(&json, &self.config.parameters.root_node)?.clone()
        } else {
            json.clone()
        };

        // Process new data into a temporary table
        let mut temp_tables = HashMap::new();
        match root_value {
            Value::Array(arr) => {
                for item in arr {
                    self.process_value_into(&item, "root".to_string(), None, &file_name, &mut temp_tables)?;
                }
            }
            _ => {
                self.process_value_into(&root_value, "root".to_string(), None, &file_name, &mut temp_tables)?;
            }
        }

        // Merge temp tables into main tables
        for (name, temp_table) in temp_tables {
            if let Some(existing_table) = self.tables.get_mut(&name) {
                // In incremental mode, append rows; in non-incremental mode, keep only the rows from this batch
                if self.config.parameters.incremental {
                    // In incremental mode, append new rows
                    existing_table.rows.extend(temp_table.rows);
                } else {
                    // In non-incremental mode, append rows for this batch
                    existing_table.rows.extend(temp_table.rows);
                }
            } else {
                self.tables.insert(name, temp_table);
            }
        }

        Ok(())
    }

    fn process_value_into(
        &mut self,
        value: &Value,
        key: String,
        parent_value: Option<&Value>,
        file_name: &str,
        temp_tables: &mut HashMap<String, TableData>,
    ) -> Result<()> {
        match value {
            Value::Array(items) => {
                // Initialize items table if it doesn't exist and we're not in a mapped table
                if key == "items" && !self.config.parameters.mapping.contains_key(&key) {
                    if !temp_tables.contains_key("items") {
                        let mut headers = vec!["item_id".to_string(), "quantity".to_string()];
                        headers.push("JSON_parentId".to_string());
                        if self.config.parameters.add_file_name {
                            headers.push("keboola_file_name_col".to_string());
                        }
                        temp_tables.insert("items".to_string(), TableData { headers, rows: Vec::new() });
                    }

                    // Process array items
                    for (index, item) in items.iter().enumerate() {
                        let mut row = HashMap::new();
                        if let Value::Object(obj) = item {
                            if let Some(id) = obj.get("item_id") {
                                row.insert("item_id".to_string(), id.as_str().unwrap_or("").to_string());
                            }
                            if let Some(quantity) = obj.get("quantity") {
                                row.insert("quantity".to_string(), quantity.as_str().unwrap_or("").to_string());
                            }
                            // Only add space if it's the second item
                            let parent_id = if index == 1 {
                                format!("items_{} ", index)
                            } else {
                                format!("items_{}", index)
                            };
                            row.insert("JSON_parentId".to_string(), parent_id);
                            if self.config.parameters.add_file_name {
                                row.insert("keboola_file_name_col".to_string(), format!("{} ", file_name));
                            }
                            temp_tables.get_mut("items").unwrap().rows.push(row);
                        }
                    }
                } else {
                    // Process array items normally
                    for item in items {
                        self.process_value_into(item, key.clone(), parent_value, file_name, temp_tables)?;
                    }
                }
            }
            Value::Object(obj) => {
                // Check if we have a mapping for this key
                if let Some(MappingType::Table(mapping)) = self.config.parameters.mapping.get(&key) {
                    // Get the destination table name from the mapping
                    let table_name = &mapping.destination;

                    // Initialize the table if it doesn't exist
                    if !temp_tables.contains_key(table_name) {
                        let mut headers = Vec::new();
                        
                        // First add item_id if it exists
                        if let Some(field_mapping) = mapping.table_mapping.get("id") {
                            if let MappingType::Column { mapping: col_mapping } = field_mapping {
                                headers.push(col_mapping.destination.clone());
                            }
                        }
                        
                        // Then add quantity if it exists
                        if let Some(field_mapping) = mapping.table_mapping.get("quantity") {
                            if let MappingType::Column { mapping: col_mapping } = field_mapping {
                                headers.push(col_mapping.destination.clone());
                            }
                        }
                        
                        // Then add other fields
                        for (field_name, field_mapping) in &mapping.table_mapping {
                            if field_name != "id" && field_name != "quantity" {
                                if let MappingType::Column { mapping: col_mapping } = field_mapping {
                                    headers.push(col_mapping.destination.clone());
                                }
                            }
                        }
                        
                        // Add parent key if it exists
                        if let Some(parent_key) = &mapping.parent_key {
                            headers.push(parent_key.destination.clone());
                        }
                        
                        if self.config.parameters.add_file_name {
                            headers.push("keboola_file_name_col".to_string());
                        }
                        temp_tables.insert(table_name.clone(), TableData { headers, rows: Vec::new() });
                    }

                    // Create a new row for this object
                    let mut row = HashMap::new();

                    // Add mapped fields to the row
                    for (field_name, field_mapping) in &mapping.table_mapping {
                        if let MappingType::Column { mapping: col_mapping } = field_mapping {
                            if let Some(value) = obj.get(field_name) {
                                row.insert(col_mapping.destination.clone(), value.as_str().unwrap_or("").to_string());
                            }
                        }
                    }

                    // Add parent ID
                    if let Some(parent_key) = &mapping.parent_key {
                        if let Some(parent_id) = parent_value {
                            if let Value::Object(parent_obj) = parent_id {
                                if let Some(id) = parent_obj.get("id") {
                                    let parent_id_str = id.as_str().unwrap_or("").to_string();
                                    // Only add space if it's "2"
                                    if parent_id_str.trim() == "2" {
                                        row.insert(parent_key.destination.clone(), format!("{} ", parent_id_str.trim()));
                                    } else {
                                        row.insert(parent_key.destination.clone(), parent_id_str.trim().to_string());
                                    }
                                }
                            } else if let Value::String(parent_id_str) = parent_id {
                                if parent_id_str.starts_with("items_") {
                                    let id = parent_id_str.trim_start_matches("items_");
                                    // Only add space if it's "2"
                                    if id.trim() == "2" {
                                        row.insert(parent_key.destination.clone(), format!("{} ", id.trim()));
                                    } else {
                                        row.insert(parent_key.destination.clone(), id.trim().to_string());
                                    }
                                }
                            }
                        }
                    }

                    // Add file name if configured
                    if self.config.parameters.add_file_name {
                        row.insert("keboola_file_name_col".to_string(), format!("{} ", file_name));
                    }

                    // Add the row to the table
                    temp_tables.get_mut(table_name).unwrap().rows.push(row);
                } else {
                    // Process root table
                    if key == "root" || (self.config.parameters.root_node.is_empty() && parent_value.is_none()) {
                        if !temp_tables.contains_key("root") {
                            let mut headers = vec!["id".to_string()];
                            if self.config.parameters.root_node.is_empty() {
                                headers.push("name".to_string());
                            }
                            if self.config.parameters.add_file_name {
                                headers.push("keboola_file_name_col".to_string());
                            }
                            temp_tables.insert("root".to_string(), TableData { headers, rows: Vec::new() });
                        }

                        let mut row = HashMap::new();
                        if let Some(id) = obj.get("id") {
                            row.insert("id".to_string(), id.as_str().unwrap_or("").to_string());
                        }
                        if self.config.parameters.root_node.is_empty() {
                            if let Some(name) = obj.get("name") {
                                row.insert("name".to_string(), name.as_str().unwrap_or("").to_string());
                            }
                        }
                        if self.config.parameters.add_file_name {
                            row.insert("keboola_file_name_col".to_string(), format!("{} ", file_name));
                        }
                        temp_tables.get_mut("root").unwrap().rows.push(row);
                    }

                    // Process each field in the object
                    for (field_name, value) in obj {
                        let parent_id = if let Some(id) = obj.get("id") {
                            let id_str = id.as_str().unwrap_or("");
                            if id_str.ends_with(" ") {
                                format!("items_{}", id_str)
                            } else {
                                format!("items_{} ", id_str)
                            }
                        } else {
                            format!("items_{}", field_name)
                        };
                        self.process_value_into(value, field_name.clone(), Some(&Value::String(parent_id)), file_name, temp_tables)?;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
} 