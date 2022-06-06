use serde_derive::{Serialize, Deserialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct UpdateDTO {
    pub url: String,
    pub schema: String,
    pub table: String,
    pub field: String,
    pub value: String,
    pub predicate: String,
    pub host: String,
    pub db: String,
    pub port: String,
    pub usr: String,
    pub pwd: String,
}
