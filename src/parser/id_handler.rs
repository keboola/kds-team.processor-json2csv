use crate::config::MappingType;
use serde_json::Value;
use std::collections::HashMap;
use super::TableData;
use super::value_processor::ValueProcessor;

pub struct IdHandler<'a> {
    tables: &'a HashMap<String, TableData>,
}

impl<'a> IdHandler<'a> {
    pub fn new(tables: &'a HashMap<String, TableData>) -> Self {
        Self { tables }
    }

    pub fn get_parent_id(&self, parent_id: &str, root_node: &str) -> String {
        // Extract the base name and index from parent_id (e.g., "items_0" -> ("items", 0))
        let parts: Vec<&str> = parent_id.rsplitn(2, '_').collect();
        if parts.len() != 2 {
            return parent_id.to_string();
        }

        let (index_str, _) = (parts[0], parts[1]);
        let index: usize = match index_str.parse() {
            Ok(i) => i,
            Err(_) => return parent_id.to_string(),
        };

        // Find the parent table
        let table_name = if root_node.is_empty() { "root" } else { root_node };
        if let Some(parent_table) = self.tables.get(table_name) {
            if index >= parent_table.rows.len() {
                return parent_id.to_string();
            }

            // Get the ID from the parent row
            if let Some(id) = parent_table.rows[index].get("id") {
                return id.to_string();
            }
        }

        parent_id.to_string()
    }

    pub fn get_object_id(obj: &Value, parent_id: Option<&str>, _table_mapping: Option<&MappingType>) -> String {
        if let Some(id) = obj.get("id") {
            return ValueProcessor::format_value(id);
        }

        if let Some(parent_id) = parent_id {
            parent_id.to_string()
        } else {
            String::new()
        }
    }
} 