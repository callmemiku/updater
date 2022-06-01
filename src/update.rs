use serde_json::Value;

#[derive(Debug, Deserialize, Serialize)]
pub struct UpdateDTO {
    pub table: String,
    pub value: String,
    pub predicate: String,
    pub url: String,
    pub usr: String,
    pub pwd: String,
}

impl UpdateDTO {
    pub fn new(message: String) -> Self {
        let v: Value = serde_json::from_str(&message)?;
        Self {
            table: v["table"].to_string(),
            value: v["value"].to_string(),
            predicate: v["predicate"].to_string(),
            url: v["url"].to_string(),
            usr: v["usr"].to_string(),
            pwd: v["pwd"].to_string()
        }
    }
}