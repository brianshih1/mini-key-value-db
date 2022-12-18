use super::data_types::DataType;

pub enum Plan {
    CreateTablePlan(CreateTablePlan),
}

pub struct CreateTablePlan {
    pub table_name: String,
    pub(crate) columns: Vec<Column>,
}

pub struct Column {
    pub column_name: String,
    pub column_type: DataType,
}
