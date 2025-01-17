#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_config() -> Config {
        Config {
            parameters: Parameters {
                mapping: HashMap::new(),
                incremental: false,
                root_node: String::new(),
                in_type: InputType::Files,
                add_file_name: false,
            },
        }
    }

    #[test]
    fn test_basic_json_conversion() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let config = create_test_config();
        let parser = Parser::new(config, temp_dir.path().to_path_buf());

        let json_content = r#"{
            "id": "1",
            "name": "Test",
            "items": [
                {"id": "item1", "value": 100},
                {"id": "item2", "value": 200}
            ]
        }"#;

        let input_file = temp_dir.path().join("test.json");
        fs::write(&input_file, json_content)?;

        parser.process_file(&input_file)?;

        // Verify the output files exist and contain correct data
        let root_csv = fs::read_to_string(temp_dir.path().join("root.csv"))?;
        assert!(root_csv.contains("id,name"));
        assert!(root_csv.contains("1,Test"));

        let items_csv = fs::read_to_string(temp_dir.path().join("items.csv"))?;
        assert!(items_csv.contains("id,value"));
        assert!(items_csv.contains("item1,100"));
        assert!(items_csv.contains("item2,200"));

        Ok(())
    }

    #[test]
    fn test_root_node_selection() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let mut config = create_test_config();
        config.parameters.root_node = "data.items".to_string();

        let parser = Parser::new(config, temp_dir.path().to_path_buf());

        let json_content = r#"{
            "data": {
                "items": [
                    {"id": "item1", "value": 100},
                    {"id": "item2", "value": 200}
                ]
            }
        }"#;

        let input_file = temp_dir.path().join("test.json");
        fs::write(&input_file, json_content)?;

        parser.process_file(&input_file)?;

        let items_csv = fs::read_to_string(temp_dir.path().join("items.csv"))?;
        assert!(items_csv.contains("id,value"));
        assert!(items_csv.contains("item1,100"));
        assert!(items_csv.contains("item2,200"));

        Ok(())
    }
} 