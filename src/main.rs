use anyhow::Result;
use std::env;
use std::fs;
use std::path::PathBuf;
use walkdir::WalkDir;

use json2csv_processor::config::{Config, InputType};
use json2csv_processor::parser;

fn main() -> Result<()> {
    let data_dir = env::var("KBC_DATADIR").unwrap_or_else(|_| "/data".to_string());

    let config_path = PathBuf::from(&data_dir).join("config.json");
    let config: Config = serde_json::from_str(&fs::read_to_string(&config_path)?)?;

    let input_dir = match config.parameters.in_type {
        InputType::Tables => PathBuf::from(&data_dir).join("in/tables"),
        InputType::Files => PathBuf::from(&data_dir).join("in/files"),
    };

    let output_dir = PathBuf::from(&data_dir).join("out/tables");

    // Create output directory if it doesn't exist
    fs::create_dir_all(&output_dir)?;

    // Process all JSON files in the input directory
    let mut parser = parser::Parser::new(config, output_dir);

    for entry in WalkDir::new(&input_dir) {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() && path.extension().is_some_and(|ext| ext == "json") {
            println!("Processing file: {}", path.display());
            if let Err(e) = parser.process_file(path) {
                eprintln!("Error processing file {}: {}", path.display(), e);
                return Err(e);
            }
        }
    }

    // Write all tables
    parser.write_tables()?;

    Ok(())
}
