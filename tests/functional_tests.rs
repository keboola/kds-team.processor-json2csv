use anyhow::Result;
use json2csv_processor::config::{Config, InputType, Parameters, MappingType, TableMapping, ColumnMapping, ParentKeyMapping};
use json2csv_processor::parser::Parser;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::error::Error;

fn setup_test_dir(test_name: &str) -> Result<PathBuf> {
    let test_dir = PathBuf::from(format!("tests/functional/{}", test_name));
    println!("Setting up test directory: {}", test_dir.display());
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
    let expected_dir = test_dir.join("expected");
    println!("Creating expected directory: {}", expected_dir.display());
    fs::create_dir_all(&expected_dir)?;

    match test_name {
        "basic-sample-2-files" => {
            let path = expected_dir.join("root.csv");
            println!("Writing expected file: {}", path.display());
            fs::write(
                path,
                "id,name\n\"1\",\"First\"\n\"2\",\"Second \"\n",
            )?;
        }
        "basic-sample-2-tables" => {
            let root_path = expected_dir.join("root.csv");
            println!("Writing expected file: {}", root_path.display());
            fs::write(
                root_path,
                "id,name\n\"1\",\"First\"\n",
            )?;
            let items_path = expected_dir.join("items.csv");
            println!("Writing expected file: {}", items_path.display());
            fs::write(items_path, "item_id,quantity,JSON_parentId\n\"1\",\"10\",\"items_0\"\n\"2\",\"20\",\"items_1 \"\n")?;
        }
        "basic-sample-2-tables-root-el" => {
            fs::write(expected_dir.join("root.csv"), "id\n\"1\"\n")?;
            fs::write(
                expected_dir.join("items.csv"),
                "item_id,quantity,JSON_parentId\n\"2 \",\"\",\"items_0\"\n",
            )?;
        }
        "sample-2-tables-add-file-name" => {
            fs::write(
                expected_dir.join("root.csv"),
                "id,name,keboola_file_name_col\n\"1\",\"Test\",\"sample.json \"\n",
            )?;
            fs::write(expected_dir.join("items.csv"), "item_id,quantity,JSON_parentId,keboola_file_name_col\n\"A\",\"10\",\"items_0\",\"sample.json \"\n\"B\",\"20\",\"items_1 \",\"sample.json \"\n")?;
        }
        "sample-2-tables-root-el-mapping" => {
            fs::write(expected_dir.join("order_items.csv"), "item_id,quantity,order_id\n\"A\",\"10\",\"1\"\n\"B\",\"20\",\"1\"\n\"C\",\"30\",\"2 \"\n")?;
        }
        "basic-sample-2-files-new-output-manifest" => {
            fs::write(
                expected_dir.join("root.csv"),
                "id,name\n\"1\",\"First\"\n\"2\",\"Second \"\n",
            )?;
        }
        "basic-sample-2-tables-new-output-manifest" => {
            fs::write(
                expected_dir.join("root.csv"),
                "id,name\n\"1\",\"First\"\n",
            )?;
            fs::write(expected_dir.join("items.csv"), "item_id,quantity,JSON_parentId\n\"1\",\"10\",\"items_0\"\n\"2\",\"20\",\"items_1 \"\n")?;
        }
        "sample-2-tables-root-el-mapping-forced" => {
            fs::write(expected_dir.join("order_items.csv"), "item_id,quantity,order_id\n\"A\",\"10\",\"1\"\n\"B\",\"20\",\"1\"\n")?;
        }
        "sample-2-tables-root-el-mapping-add-filename" => {
            fs::write(expected_dir.join("order_items.csv"), "item_id,quantity,order_id,keboola_file_name_col\n\"A\",\"10\",\"1\",\"sample.json \"\n\"B\",\"20\",\"1\",\"sample.json \"\n")?;
        }
        "incremental-loading" => {
            fs::write(
                expected_dir.join("root.csv"),
                "id,name\n\"1\",\"First\"\n\"2\",\"Second\"\n",
            )?;
            fs::write(expected_dir.join("items.csv"), "item_id,quantity,JSON_parentId\n\"A\",\"10\",\"items_0\"\n\"B\",\"20\",\"items_0\"\n")?;
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
    // Ensure parent directories exist
    if let Some(parent) = expected_path.parent() {
        fs::create_dir_all(parent)?;
    }
    if let Some(parent) = actual_path.parent() {
        fs::create_dir_all(parent)?;
    }

    println!("Checking file existence:");
    println!("Actual path exists: {}", actual_path.exists());
    println!("Expected path exists: {}", expected_path.exists());

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

    let mut parser = Parser::new(config, test_dir.join("out/tables"))?;
    parser.process_file(&test_dir.join("in/files/sample1.json"))?;
    parser.process_file(&test_dir.join("in/files/sample2.json"))?;
    parser.write_tables()?;

    compare_csv_files(
        &test_dir.join("out/tables/root.csv"),
        &test_dir.join("expected/root.csv"),
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

    let mut parser = Parser::new(config, test_dir.join("out/tables"))?;
    parser.process_file(&test_dir.join("in/tables/sample.json"))?;
    parser.write_tables()?;

    // Check if output files exist
    let root_csv = test_dir.join("out/tables/root.csv");
    let items_csv = test_dir.join("out/tables/items.csv");
    println!("Checking output files:");
    println!("root.csv exists: {}", root_csv.exists());
    println!("items.csv exists: {}", items_csv.exists());

    // Copy output files to a temporary location
    let temp_dir = test_dir.join("temp");
    fs::create_dir_all(&temp_dir)?;
    fs::copy(&root_csv, temp_dir.join("root.csv"))?;
    fs::copy(&items_csv, temp_dir.join("items.csv"))?;

    compare_csv_files(
        &temp_dir.join("root.csv"),
        &test_dir.join("expected/root.csv"),
    )?;

    compare_csv_files(
        &temp_dir.join("items.csv"),
        &test_dir.join("expected/items.csv"),
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
                        "item_id": "2 ",
                        "quantity": ""
                    }
                ]
            }
        })
        .to_string(),
    )?;

    let mut parser = Parser::new(config, test_dir.join("out/tables"))?;
    parser.process_file(&test_dir.join("in/tables/sample.json"))?;
    parser.write_tables()?;

    // Copy output files to a temporary location
    let temp_dir = test_dir.join("temp");
    fs::create_dir_all(&temp_dir)?;
    fs::copy(
        test_dir.join("out/tables/root.csv"),
        temp_dir.join("root.csv"),
    )?;
    fs::copy(
        test_dir.join("out/tables/items.csv"),
        temp_dir.join("items.csv"),
    )?;

    compare_csv_files(
        &temp_dir.join("root.csv"),
        &test_dir.join("expected/root.csv"),
    )?;

    compare_csv_files(
        &temp_dir.join("items.csv"),
        &test_dir.join("expected/items.csv"),
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
                    "item_id": "A",
                    "quantity": "10"
                },
                {
                    "item_id": "B",
                    "quantity": "20"
                }
            ]
        })
        .to_string(),
    )?;

    let mut parser = Parser::new(config, test_dir.join("out/tables"))?;
    parser.process_file(&test_dir.join("in/tables/sample.json"))?;
    parser.write_tables()?;

    // Copy output files to a temporary location
    let temp_dir = test_dir.join("temp");
    fs::create_dir_all(&temp_dir)?;
    fs::copy(
        test_dir.join("out/tables/root.csv"),
        temp_dir.join("root.csv"),
    )?;
    fs::copy(
        test_dir.join("out/tables/items.csv"),
        temp_dir.join("items.csv"),
    )?;

    compare_csv_files(
        &temp_dir.join("root.csv"),
        &test_dir.join("expected/root.csv"),
    )?;

    compare_csv_files(
        &temp_dir.join("items.csv"),
        &test_dir.join("expected/items.csv"),
    )?;

    Ok(())
}

#[test]
fn test_sample_with_mapping() -> Result<()> {
    let test_dir = setup_test_dir("sample-2-tables-root-el-mapping")?;

    let config = Config {
        parameters: Parameters {
            in_type: InputType::Tables,
            mapping: {
                let mut mapping = HashMap::new();
                mapping.insert("items".to_string(), MappingType::Table(TableMapping {
                    destination: "order_items".to_string(),
                    parent_key: Some(ParentKeyMapping {
                        destination: "order_id".to_string(),
                        primary_key: false,
                    }),
                    table_mapping: {
                        let mut table_mapping = HashMap::new();
                        table_mapping.insert("quantity".to_string(), MappingType::Column { mapping: ColumnMapping {
                            destination: "quantity".to_string(),
                            primary_key: false,
                        }});
                        table_mapping.insert("id".to_string(), MappingType::Column { mapping: ColumnMapping {
                            destination: "item_id".to_string(),
                            primary_key: true,
                        }});
                        table_mapping
                    },
                }));
                mapping
            },
            incremental: false,
            root_node: "data".to_string(),
            add_file_name: false,
        },
    };

    // Create input file
    let input_file = test_dir.join("in/tables/sample.json");
    fs::create_dir_all(input_file.parent().unwrap())?;
    fs::write(&input_file, r#"{
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
                "id": "2 ",
                "items": [
                    {
                        "id": "C",
                        "quantity": "30"
                    }
                ]
            }
        ]
    }"#)?;

    // Create expected output files
    let expected_dir = test_dir.join("expected");
    fs::create_dir_all(&expected_dir)?;

    let expected_order_items = expected_dir.join("order_items.csv");
    fs::write(&expected_order_items, r#"item_id,quantity,order_id
"A","10","1"
"B","20","1"
"C","30","2 ""#)?;

    let mut parser = Parser::new(config, test_dir.join("out/tables"))?;
    parser.process_file(&input_file)?;
    parser.write_tables()?;

    compare_csv_files(
        &test_dir.join("out/tables/order_items.csv"),
        &test_dir.join("expected/order_items.csv"),
    )?;

    Ok(())
}

#[test]
fn test_array_input() -> Result<()> {
    let test_dir = setup_test_dir("array-input")?;
    fs::create_dir_all(test_dir.join("expected"))?;
    fs::create_dir_all(test_dir.join("in/files"))?;

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
        test_dir.join("in/files/sample.json"),
        json!([
            {
                "id": "1",
                "name": "First"
            },
            {
                "id": "2",
                "name": "Second "
            }
        ])
        .to_string(),
    )?;

    fs::write(
        test_dir.join("expected/root.csv"),
        "id,name\n\"1\",\"First\"\n\"2\",\"Second \"\n",
    )?;

    let mut parser = Parser::new(config, test_dir.join("out/tables"))?;
    parser.process_file(&test_dir.join("in/files/sample.json"))?;
    parser.write_tables()?;

    compare_csv_files(
        &test_dir.join("out/tables/root.csv"),
        &test_dir.join("expected/root.csv"),
    )?;

    Ok(())
}

#[test]
fn test_basic_sample_2_files_with_manifest() -> Result<()> {
    let test_dir = setup_test_dir("basic-sample-2-files-new-output-manifest")?;

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

    let mut parser = Parser::new(config, test_dir.join("out/tables"))?;
    parser.process_file(&test_dir.join("in/files/sample1.json"))?;
    parser.process_file(&test_dir.join("in/files/sample2.json"))?;
    parser.write_tables()?;

    // Write manifest file
    fs::write(
        test_dir.join("out/tables/root.csv.manifest"),
        json!({
            "primary_key": ["id"],
            "incremental": false,
            "columns": ["id", "name"],
            "metadata": []
        })
        .to_string(),
    )?;

    compare_csv_files(
        &test_dir.join("out/tables/root.csv"),
        &test_dir.join("expected/root.csv"),
    )?;

    Ok(())
}

#[test]
fn test_basic_sample_2_tables_with_manifest() -> Result<()> {
    let test_dir = setup_test_dir("basic-sample-2-tables-new-output-manifest")?;

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

    let mut parser = Parser::new(config, test_dir.join("out/tables"))?;
    parser.process_file(&test_dir.join("in/tables/sample.json"))?;
    parser.write_tables()?;

    // Copy output files to a temporary location
    let temp_dir = test_dir.join("temp");
    fs::create_dir_all(&temp_dir)?;
    fs::copy(
        test_dir.join("out/tables/root.csv"),
        temp_dir.join("root.csv"),
    )?;
    fs::copy(
        test_dir.join("out/tables/items.csv"),
        temp_dir.join("items.csv"),
    )?;

    compare_csv_files(
        &temp_dir.join("root.csv"),
        &test_dir.join("expected/root.csv"),
    )?;

    compare_csv_files(
        &temp_dir.join("items.csv"),
        &test_dir.join("expected/items.csv"),
    )?;

    Ok(())
}

#[test]
fn test_sample_with_forced_mapping() -> Result<()> {
    let test_dir = setup_test_dir("sample-2-tables-root-el-mapping-forced")?;

    let config = Config {
        parameters: Parameters {
            in_type: InputType::Tables,
            mapping: {
                let mut mapping = HashMap::new();
                mapping.insert("items".to_string(), MappingType::Table(TableMapping {
                    destination: "order_items".to_string(),
                    parent_key: Some(ParentKeyMapping {
                        destination: "order_id".to_string(),
                        primary_key: false,
                    }),
                    table_mapping: {
                        let mut table_mapping = HashMap::new();
                        table_mapping.insert("quantity".to_string(), MappingType::Column { mapping: ColumnMapping {
                            destination: "quantity".to_string(),
                            primary_key: false,
                        }});
                        table_mapping.insert("id".to_string(), MappingType::Column { mapping: ColumnMapping {
                            destination: "item_id".to_string(),
                            primary_key: true,
                        }});
                        table_mapping
                    },
                }));
                mapping
            },
            incremental: false,
            root_node: "data".to_string(),
            add_file_name: false,
        },
    };

    // Create input file
    let input_file = test_dir.join("in/tables/sample.json");
    fs::create_dir_all(input_file.parent().unwrap())?;
    fs::write(&input_file, r#"{
        "data": {
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
        }
    }"#)?;

    // Create expected output files
    let expected_dir = test_dir.join("expected");
    fs::create_dir_all(&expected_dir)?;

    let expected_order_items = expected_dir.join("order_items.csv");
    fs::write(&expected_order_items, r#"item_id,quantity,order_id
"A","10","1"
"B","20","1""#)?;

    let mut parser = Parser::new(config, test_dir.join("out/tables"))?;
    parser.process_file(&input_file)?;
    parser.write_tables()?;

    compare_csv_files(
        &test_dir.join("out/tables/order_items.csv"),
        &test_dir.join("expected/order_items.csv"),
    )?;

    Ok(())
}

#[test]
fn test_sample_with_root_node_mapping_and_filename() -> Result<()> {
    let test_dir = setup_test_dir("sample-2-tables-root-el-mapping-add-filename")?;

    let config = Config {
        parameters: Parameters {
            in_type: InputType::Tables,
            mapping: {
                let mut mapping = HashMap::new();
                mapping.insert("items".to_string(), MappingType::Table(TableMapping {
                    destination: "order_items".to_string(),
                    parent_key: Some(ParentKeyMapping {
                        destination: "order_id".to_string(),
                        primary_key: false,
                    }),
                    table_mapping: {
                        let mut table_mapping = HashMap::new();
                        table_mapping.insert("quantity".to_string(), MappingType::Column { mapping: ColumnMapping {
                            destination: "quantity".to_string(),
                            primary_key: false,
                        }});
                        table_mapping.insert("id".to_string(), MappingType::Column { mapping: ColumnMapping {
                            destination: "item_id".to_string(),
                            primary_key: true,
                        }});
                        table_mapping
                    },
                }));
                mapping
            },
            incremental: false,
            root_node: "data".to_string(),
            add_file_name: true,
        },
    };

    // Create input file
    let input_file = test_dir.join("in/tables/sample.json");
    fs::create_dir_all(input_file.parent().unwrap())?;
    fs::write(&input_file, r#"{
        "data": {
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
        }
    }"#)?;

    // Create expected output files
    let expected_dir = test_dir.join("expected");
    fs::create_dir_all(&expected_dir)?;

    let expected_order_items = expected_dir.join("order_items.csv");
    fs::write(&expected_order_items, r#"item_id,quantity,order_id,keboola_file_name_col
"A","10","1","sample.json "
"B","20","1","sample.json ""#)?;

    let mut parser = Parser::new(config, test_dir.join("out/tables"))?;
    parser.process_file(&input_file)?;
    parser.write_tables()?;

    compare_csv_files(
        &test_dir.join("out/tables/order_items.csv"),
        &test_dir.join("expected/order_items.csv"),
    )?;

    Ok(())
}

#[test]
fn test_incremental_loading() -> Result<()> {
    let test_dir = setup_test_dir("incremental-loading")?;

    let config = Config {
        parameters: Parameters {
            in_type: InputType::Tables,
            root_node: String::new(),
            incremental: true,
            add_file_name: false,
            mapping: HashMap::new(),
        },
    };

    create_config(&test_dir, &config)?;

    // First batch of data
    fs::write(
        test_dir.join("in/tables/sample1.json"),
        json!({
            "id": "1",
            "name": "First",
            "items": [
                {
                    "item_id": "A",
                    "quantity": "10"
                }
            ]
        })
        .to_string(),
    )?;

    let mut parser = Parser::new(config.clone(), test_dir.join("out/tables"))?;
    parser.process_file(&test_dir.join("in/tables/sample1.json"))?;
    parser.write_tables()?;

    // Second batch of data
    fs::write(
        test_dir.join("in/tables/sample2.json"),
        json!({
            "id": "2",
            "name": "Second",
            "items": [
                {
                    "item_id": "B",
                    "quantity": "20"
                }
            ]
        })
        .to_string(),
    )?;

    let mut parser = Parser::new(config, test_dir.join("out/tables"))?;
    parser.process_file(&test_dir.join("in/tables/sample2.json"))?;
    parser.write_tables()?;

    // Write expected files
    fs::write(
        test_dir.join("expected/root.csv"),
        "id,name\n\"1\",\"First\"\n\"2\",\"Second\"\n",
    )?;
    fs::write(
        test_dir.join("expected/items.csv"),
        "item_id,quantity,JSON_parentId\n\"A\",\"10\",\"items_0\"\n\"B\",\"20\",\"items_0\"\n",
    )?;

    compare_csv_files(
        &test_dir.join("out/tables/root.csv"),
        &test_dir.join("expected/root.csv"),
    )?;

    compare_csv_files(
        &test_dir.join("out/tables/items.csv"),
        &test_dir.join("expected/items.csv"),
    )?;

    Ok(())
}
