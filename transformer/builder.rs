use crate::transformer::spec::{ColSpec, CsvTransformer, OnInvalid};
use crate::transformer::validator::Validator;
use polars::prelude::*;

#[derive(Debug, Clone)]
pub struct ColumnBuilder {
    pub name: String,
    pub dtype: DataType,
    pub validators: Vec<Validator>,
    pub on_invalid: OnInvalid,
}

impl ColumnBuilder {
    pub fn new<S: Into<String>>(name: S, dtype: DataType) -> Self {
        Self {
            name: name.into(),
            dtype,
            validators: Vec::new(),
            on_invalid: OnInvalid::Error,
        }
    }

    pub fn ignore_invalid_type(mut self) -> Self {
        self.on_invalid = OnInvalid::Ignore;
        self
    }

    pub fn to_col_spec(self) -> ColSpec {
        ColSpec {
            name: self.name,
            dtype: self.dtype,
            validators: self.validators,
            on_invalid: self.on_invalid,
        }
    }
}

pub struct CsvTransformerBuilder {
    columns: Vec<ColumnBuilder>,
}

impl CsvTransformerBuilder {
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    pub fn add_column(mut self, col: ColumnBuilder) -> Self {
        self.columns.push(col);
        self
    }

    pub fn build(self) -> CsvTransformer {
        let cols = self
            .columns
            .into_iter()
            .map(|col| col.to_col_spec())
            .collect();

        CsvTransformer { cols }
    }
}
