pub enum DataType {
    STRING,
    NUMBER,
}

impl DataType {
    pub fn as_str(&self) -> &'static str {
        match self {
            DataType::NUMBER => "NUMBER",
            DataType::STRING => "STRING",
        }
    }
}
