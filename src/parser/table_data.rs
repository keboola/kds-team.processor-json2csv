use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct TableData {
    pub headers: Vec<String>,
    pub rows: Vec<HashMap<String, String>>,
}

impl TableData {
    pub fn new(headers: Vec<String>) -> Self {
        Self {
            headers,
            rows: Vec::new(),
        }
    }

    pub fn add_row(&mut self, row: HashMap<String, String>) {
        self.rows.push(row);
    }

    pub fn ensure_header(&mut self, header: String) {
        if !self.headers.contains(&header) {
            self.headers.push(header);
        }
    }

    pub fn get_sorted_unique_rows(&self) -> Vec<HashMap<String, String>> {
        let mut sorted_rows = self.rows.clone();
        sorted_rows.sort_by(|a, b| {
            let a_id = a.get("id")
                .or_else(|| a.get("item_id"))
                .map(String::as_str)
                .unwrap_or("");
            let b_id = b.get("id")
                .or_else(|| b.get("item_id"))
                .map(String::as_str)
                .unwrap_or("");
            a_id.cmp(b_id)
        });

        let mut unique_rows = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for row in sorted_rows {
            let key = row.get("id")
                .or_else(|| row.get("item_id"))
                .map(String::as_str)
                .unwrap_or("");
            if seen.insert(key.to_string()) {
                unique_rows.push(row);
            }
        }
        unique_rows
    }
} 