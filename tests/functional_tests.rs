use anyhow::Result;
use json2csv_processor::config::{Config, InputType, Parameters};
use json2csv_processor::parser::Parser;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

fn setup_test_dir(test_name: &str) -> Result<PathBuf> {
    let test_dir = PathBuf::from(format!("tests/functional/{}", test_name));
    fs::create_dir_all(&test_dir)?;

    // Copy source files to in/files or in/tables
    let source_dir = PathBuf::from(format!("tests/functional/{}/source", test_name));
    if source_dir.exists() {
        for entry in fs::read_dir(source_dir)? {
            let entry = entry?;
            let source_path = entry.path();
            if source_path.is_file() {
                let file_name = source_path.file_name().unwrap();
                let in_files = test_dir.join("in/files");
                let in_tables = test_dir.join("in/tables");
                fs::create_dir_all(&in_files)?;
                fs::create_dir_all(&in_tables)?;
                fs::copy(&source_path, in_files.join(file_name))?;
                fs::copy(&source_path, in_tables.join(file_name))?;
            }
        }
    }

    // Create output directory
    fs::create_dir_all(test_dir.join("out/tables"))?;

    // Create expected directory and write expected files
    fs::create_dir_all(test_dir.join("expected"))?;

    match test_name {
        "basic-sample-2-files" => {
            fs::write(
                test_dir.join("expected/root.csv"),
                "id,name\n\"1\",\"First\"\n\"2\",\"Second \"\n",
            )?;
        }
        "basic-sample-2-tables" => {
            fs::write(
                test_dir.join("expected/root.csv"),
                "id,name\n\"1\",\"First\"\n",
            )?;
            fs::write(test_dir.join("expected/items.csv"), "item_id,quantity,JSON_parentId\n\"1\",\"10\",\"items_0\"\n\"2\",\"20\",\"items_1 \"\n")?;
        }
        "basic-sample-2-tables-root-el" => {
            fs::write(test_dir.join("expected/root.csv"), "id\n\"1\"\n")?;
            fs::write(
                test_dir.join("expected/items.csv"),
                "item_id,quantity,JSON_parentId\n\"2 \",\"\",\"items_0\"\n",
            )?;
        }
        "sample-2-tables-add-file-name" => {
            fs::write(
                test_dir.join("expected/root.csv"),
                "id,name,keboola_file_name_col\n\"1\",\"Test\",\"sample.json \"\n",
            )?;
            fs::write(test_dir.join("expected/items.csv"), "item_id,quantity,JSON_parentId\n\"A\",\"10\",\"items_0\"\n\"B\",\"20\",\"items_1 \"\n")?;
        }
        "sample-2-tables-root-el-mapping" => {
            fs::write(test_dir.join("expected/order_items.csv"), "item_id,quantity,order_id\n\"A\",\"10\",\"1\"\n\"B\",\"20\",\"1\"\n\"C\",\"30\",\"2 \"\n")?;
        }
        _ => {}
    }

    Ok(test_dir)
}

fn create_config(test_dir: &Path, config: &Config) -> Result<()> {
    let config_path = test_dir.join("config.json");
    fs::write(config_path, serde_json::to_string_pretty(&config)?)?;
    Ok(())
}

fn compare_csv_files(actual_path: &Path, expected_path: &Path) -> Result<()> {
    let actual_content = fs::read_to_string(actual_path)?;
    let expected_content = fs::read_to_string(expected_path)?;

    println!("Comparing files:");
    println!("Actual path: {}", actual_path.display());
    println!("Expected path: {}", expected_path.display());
    println!("Actual content:\n{}", actual_content);
    println!("Expected content:\n{}", expected_content);

    let mut actual_reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(actual_content.as_bytes());
    let mut expected_reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(expected_content.as_bytes());

    let actual_headers = actual_reader.headers()?.clone();
    let expected_headers = expected_reader.headers()?.clone();

    assert_eq!(actual_headers, expected_headers, "Headers don't match");

    let actual_rows: Vec<_> = actual_reader.records().collect::<Result<_, _>>()?;
    let expected_rows: Vec<_> = expected_reader.records().collect::<Result<_, _>>()?;

    assert_eq!(
        actual_rows.len(),
        expected_rows.len(),
        "Row count doesn't match"
    );

    for (actual_row, expected_row) in actual_rows.iter().zip(expected_rows.iter()) {
        assert_eq!(actual_row, expected_row, "Rows don't match");
    }

    Ok(())
}

#[test]
fn test_basic_sample_2_files() -> Result<()> {
    let test_dir = setup_test_dir("basic-sample-2-files")?;

    let config = Config {
        parameters: Parameters {
            in_type: InputType::Files,
            root_node: String::new(),
            incremental: false,
            add_file_name: false,
            mapping: HashMap::new(),
        },
    };

    create_config(&test_dir, &config)?;

    fs::write(
        test_dir.join("in/files/sample1.json"),
        json!({
            "id": "1",
            "name": "First"
        })
        .to_string(),
    )?;

    fs::write(
        test_dir.join("in/files/sample2.json"),
        json!({
            "id": "2",
            "name": "Second "
        })
        .to_string(),
    )?;

    let mut parser = Parser::new(config, test_dir.join("out/tables"));
    parser.process_file(&test_dir.join("in/files/sample1.json"))?;
    parser.process_file(&test_dir.join("in/files/sample2.json"))?;
    parser.write_tables()?;

    compare_csv_files(
        &test_dir.join("out/tables/root.csv"),
        &PathBuf::from("tests/functional/basic-sample-2-files/expected/root.csv"),
    )?;

    Ok(())
}

#[test]
fn test_basic_sample_2_tables() -> Result<()> {
    let test_dir = setup_test_dir("basic-sample-2-tables")?;

    let config = Config {
        parameters: Parameters {
            in_type: InputType::Tables,
            root_node: String::new(),
            incremental: false,
            add_file_name: false,
            mapping: HashMap::new(),
        },
    };

    create_config(&test_dir, &config)?;

    fs::write(
        test_dir.join("in/tables/sample.json"),
        json!({
            "id": "1",
            "name": "First",
            "items": [
                {
                    "item_id": "1",
                    "quantity": "10"
                },
                {
                    "item_id": "2",
                    "quantity": "20"
                }
            ]
        })
        .to_string(),
    )?;

    let mut parser = Parser::new(config, test_dir.join("out/tables"));
    parser.process_file(&test_dir.join("in/tables/sample.json"))?;
    parser.write_tables()?;

    compare_csv_files(
        &test_dir.join("out/tables/root.csv"),
        &PathBuf::from("tests/functional/basic-sample-2-tables/expected/root.csv"),
    )?;

    compare_csv_files(
        &test_dir.join("out/tables/items.csv"),
        &PathBuf::from("tests/functional/basic-sample-2-tables/expected/items.csv"),
    )?;

    Ok(())
}

#[test]
fn test_sample_with_root_node() -> Result<()> {
    let test_dir = setup_test_dir("basic-sample-2-tables-root-el")?;

    let config = Config {
        parameters: Parameters {
            in_type: InputType::Tables,
            root_node: "data".to_string(),
            incremental: false,
            add_file_name: false,
            mapping: HashMap::new(),
        },
    };

    create_config(&test_dir, &config)?;

    fs::write(
        test_dir.join("in/tables/sample.json"),
        json!({
            "data": {
                "id": "1",
                "items": [
                    {
                        "id": "2 "
                    }
                ]
            }
        })
        .to_string(),
    )?;

    let mut parser = Parser::new(config, test_dir.join("out/tables"));
    parser.process_file(&test_dir.join("in/tables/sample.json"))?;
    parser.write_tables()?;

    compare_csv_files(
        &test_dir.join("out/tables/root.csv"),
        &PathBuf::from("tests/functional/basic-sample-2-tables-root-el/expected/root.csv"),
    )?;

    compare_csv_files(
        &test_dir.join("out/tables/items.csv"),
        &PathBuf::from("tests/functional/basic-sample-2-tables-root-el/expected/items.csv"),
    )?;

    Ok(())
}

#[test]
fn test_sample_with_file_name() -> Result<()> {
    let test_dir = setup_test_dir("sample-2-tables-add-file-name")?;

    let config = Config {
        parameters: Parameters {
            in_type: InputType::Tables,
            root_node: String::new(),
            incremental: false,
            add_file_name: true,
            mapping: HashMap::new(),
        },
    };

    create_config(&test_dir, &config)?;

    fs::write(
        test_dir.join("in/tables/sample.json"),
        json!({
            "id": "1",
            "name": "Test",
            "items": [
                {
                    "id": "A",
                    "quantity": "10"
                },
                {
                    "id": "B",
                    "quantity": "20"
                }
            ]
        })
        .to_string(),
    )?;

    let mut parser = Parser::new(config, test_dir.join("out/tables"));
    parser.process_file(&test_dir.join("in/tables/sample.json"))?;
    parser.write_tables()?;

    compare_csv_files(
        &test_dir.join("out/tables/root.csv"),
        &PathBuf::from("tests/functional/sample-2-tables-add-file-name/expected/root.csv"),
    )?;

    compare_csv_files(
        &test_dir.join("out/tables/items.csv"),
        &PathBuf::from("tests/functional/sample-2-tables-add-file-name/expected/items.csv"),
    )?;

    Ok(())
}

#[test]
fn test_sample_with_mapping() -> Result<()> {
    let test_dir = setup_test_dir("sample-2-tables-root-el-mapping")?;

    let mut mapping = HashMap::new();
    let json_mapping = json!({
        "id": {
            "type": "column",
            "mapping": {
                "destination": "order_id",
                "primary_key": true
            }
        },
        "items": {
            "type": "table",
            "destination": "order_items",
            "parent_key": {
                "destination": "order_id",
                "primary_key": true
            },
            "tableMapping": {
                "id": {
                    "type": "column",
                    "mapping": {
                        "destination": "item_id",
                        "primary_key": true
                    }
                },
                "quantity": {
                    "type": "column",
                    "mapping": {
                        "destination": "quantity"
                    }
                }
            }
        }
    });

    // Convert JSON mapping to HashMap<String, MappingType>
    if let Value::Object(obj) = json_mapping {
        for (key, value) in obj {
            mapping.insert(key, serde_json::from_value(value)?);
        }
    }

    let config = Config {
        parameters: Parameters {
            in_type: InputType::Tables,
            root_node: "data".to_string(),
            incremental: false,
            add_file_name: false,
            mapping,
        },
    };

    create_config(&test_dir, &config)?;

    fs::write(
        test_dir.join("in/tables/sample.json"),
        json!({
            "data": [
                {
                    "id": "1",
                    "items": [
                        {
                            "id": "A",
                            "quantity": "10"
                        },
                        {
                            "id": "B",
                            "quantity": "20"
                        }
                    ]
                },
                {
                    "id": "2",
                    "items": [
                        {
                            "id": "C",
                            "quantity": "30"
                        }
                    ]
                }
            ]
        })
        .to_string(),
    )?;

    let mut parser = Parser::new(config, test_dir.join("out/tables"));
    parser.process_file(&test_dir.join("in/tables/sample.json"))?;
    parser.write_tables()?;

    compare_csv_files(
        &test_dir.join("out/tables/order_items.csv"),
        &PathBuf::from("tests/functional/sample-2-tables-root-el-mapping/expected/order_items.csv"),
    )?;

    Ok(())
}
