use std::fs;
use std::io::Write;
use std::path::Path;
use anyhow::Result;
use super::TableData;

pub struct CsvWriter;

impl CsvWriter {
    pub fn write_table(table_data: &TableData, output_path: &Path) -> Result<()> {
        // Create parent directory if it doesn't exist
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Write headers without quotes
        let mut file = fs::File::create(output_path)?;
        writeln!(file, "{}", table_data.headers.join(","))?;

        // Write rows with quotes
        let mut writer = csv::WriterBuilder::new()
            .quote_style(csv::QuoteStyle::Always)
            .has_headers(false)
            .from_writer(file);

        let unique_rows = table_data.get_sorted_unique_rows();

        for row in &unique_rows {
            let record: Vec<_> = table_data
                .headers
                .iter()
                .map(|header| row.get(header).map(String::as_str).unwrap_or(""))
                .collect();
            writer.write_record(&record)?;
        }

        writer.flush()?;
        Ok(())
    }

    pub fn clear_directory(dir: &Path) -> Result<()> {
        if !dir.exists() {
            fs::create_dir_all(dir)?;
            return Ok(());
        }

        // Only clear if it's an output directory and not an expected directory
        let dir_str = dir.to_string_lossy();
        if dir_str.contains("/out/tables") && !dir_str.contains("/expected") {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() && path.extension().map_or(false, |ext| ext == "csv") {
                    fs::remove_file(path)?;
                }
            }
        }
        Ok(())
    }
} 