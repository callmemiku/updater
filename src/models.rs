use serde_derive::{Serialize, Deserialize};

trait withDB {
    
}

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

#[derive(Debug, Deserialize, Serialize)]
pub struct UpdateFromViewDTO {
    pub schema: String,
    pub table: String,
    pub view: String,
    pub field: String,
    pub host: String,
    pub db: String,
    pub port: String,
    pub usr: String,
    pub pwd: String,
    pub view_field: String,
}