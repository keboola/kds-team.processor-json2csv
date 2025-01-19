mod table_data;
mod value_processor;
mod id_handler;
mod writer;
mod processor;

pub use table_data::TableData;

use crate::config::Config;
use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub struct Parser {
    config: Config,
    output_dir: PathBuf,
    tables: HashMap<String, TableData>,
    has_loaded_existing: bool,
}

impl Parser {
    pub fn new(config: Config, output_dir: PathBuf) -> Result<Self> {
        Ok(Self {
            config,
            output_dir,
            tables: HashMap::new(),
            has_loaded_existing: false,
        })
    }

    pub fn process_file(&mut self, input_path: &Path) -> Result<()> {
        let mut processor = processor::Processor::new(
            &mut self.tables,
            &self.config,
            &mut self.has_loaded_existing,
        );
        processor.process_file(input_path, &self.output_dir)
    }

    pub fn write_tables(&self) -> Result<()> {
        if !self.config.parameters.incremental {
            writer::CsvWriter::clear_directory(&self.output_dir)?;
        }

        for (table_name, table_data) in &self.tables {
            let output_path = self.output_dir.join(format!("{}.csv", table_name));
            writer::CsvWriter::write_table(table_data, &output_path)?;
        }
        Ok(())
    }
} 