use polars::prelude::*;

#[derive(Debug, Clone)]
pub enum Validator {
    NotNull,
    NumGreaterThan(f64),
    NumLessThan(f64),
    TextStartsWith(String),
}

impl Validator {
    pub fn ok(&self, val: &AnyValue) -> bool {
        match (self, val) {
            (Validator::NotNull, AnyValue::Null) => false,
            (Validator::NotNull, _) => true,

            (Validator::NumGreaterThan(min), AnyValue::Int64(x)) => (*x as f64) > *min,
            (Validator::NumGreaterThan(min), AnyValue::Float64(x)) => *x > *min,

            (Validator::NumLessThan(max), AnyValue::Int64(x)) => (*x as f64) < *max,
            (Validator::NumLessThan(max), AnyValue::Float64(x)) => *x < *max,

            (Validator::TextStartsWith(prefixo), AnyValue::String(s)) => s.starts_with(prefixo),

            _ => true,
        }
    }
}
