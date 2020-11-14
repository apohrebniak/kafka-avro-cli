use serde_json::Value as JsonValue;
use std::io::{BufRead, Read};
use std::{fs, io};

pub fn read_payload(path: &str) -> io::Result<Vec<String>> {
    let file = fs::File::open(path)?;
    io::BufReader::new(file).lines().collect()
}

pub fn parse_json(s: &str) -> serde_json::Result<JsonValue> {
    serde_json::from_str(s)
}
