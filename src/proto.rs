use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum Command {
    Get,
    Set,
    Remove,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Record {
    pub cmd: Command,
    pub key: String,
    pub value: String,
}
