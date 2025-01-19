use anyhow::{Context, Result};
use json2csv_processor::config::Config;
use json2csv_processor::parser::Parser;
use std::env;
use std::fs;
use std::path::PathBuf;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <data_dir>", args[0]);
        std::process::exit(1);
    }

    let data_dir = PathBuf::from(&args[1]);
    let config_path = data_dir.join("config.json");
    let config_str = fs::read_to_string(&config_path)
        .with_context(|| format!("Failed to read config file: {}", config_path.display()))?;

    let config: Config = serde_json::from_str(&config_str)
        .with_context(|| format!("Failed to parse config file: {}", config_path.display()))?;

    let input_dir = data_dir.join("in");
    let output_dir = data_dir.join("out/tables");

    // Create output directory if it doesn't exist
    fs::create_dir_all(&output_dir)?;

    // Process files based on input type
    let input_path = match config.parameters.in_type {
        json2csv_processor::config::InputType::Files => input_dir.join("files"),
        json2csv_processor::config::InputType::Tables => input_dir.join("tables"),
    };

    let mut parser = Parser::new(config, output_dir)?;

    for entry in fs::read_dir(input_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "json") {
            if let Err(e) = parser.process_file(&path) {
                eprintln!("Error processing file {}: {}", path.display(), e);
                std::process::exit(1);
            }
        }
    }

    parser.write_tables()?;

    Ok(())
}
