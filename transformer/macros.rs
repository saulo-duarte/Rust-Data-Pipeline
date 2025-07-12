#[macro_export]
macro_rules! csv_schema {
    ($($col:expr),* $(,)?) => {{
        use crate::transformer::builder::CsvTransformerBuilder;
        let mut builder = CsvTransformerBuilder::new();
        $(
            builder = builder.add_column($col);
        )*
        builder.build()
    }};
}

#[macro_export]
macro_rules! text_col {
    // Caso base - apenas nome
    ($name:expr) => {{
        use crate::transformer::builder::ColumnBuilder;
        use polars::prelude::DataType;
        ColumnBuilder::new($name, DataType::String)
    }};

    // Apenas mandatory
    ($name:expr, mandatory) => {{
        use crate::transformer::builder::ColumnBuilder;
        use crate::transformer::validator::Validator;
        use polars::prelude::DataType;
        let mut col = ColumnBuilder::new($name, DataType::String);
        col.validators.push(Validator::NotNull);
        col
    }};

    // Apenas starts_with
    ($name:expr, starts_with($prefix:expr)) => {{
        use crate::transformer::builder::ColumnBuilder;
        use crate::transformer::validator::Validator;
        use polars::prelude::DataType;
        let mut col = ColumnBuilder::new($name, DataType::String);
        col.validators
            .push(Validator::TextStartsWith($prefix.into()));
        col
    }};

    // Apenas ignore_invalid
    ($name:expr, ignore_invalid) => {{
        use crate::transformer::builder::ColumnBuilder;
        use polars::prelude::DataType;
        ColumnBuilder::new($name, DataType::String).ignore_invalid_type()
    }};

    // mandatory + starts_with
    ($name:expr, mandatory, starts_with($prefix:expr)) => {{
        use crate::transformer::builder::ColumnBuilder;
        use crate::transformer::validator::Validator;
        use polars::prelude::DataType;
        let mut col = ColumnBuilder::new($name, DataType::String);
        col.validators.push(Validator::NotNull);
        col.validators
            .push(Validator::TextStartsWith($prefix.into()));
        col
    }};

    // mandatory + ignore_invalid
    ($name:expr, mandatory, ignore_invalid) => {{
        use crate::transformer::builder::ColumnBuilder;
        use crate::transformer::validator::Validator;
        use polars::prelude::DataType;
        let mut col = ColumnBuilder::new($name, DataType::String);
        col.validators.push(Validator::NotNull);
        col.ignore_invalid_type()
    }};

    // starts_with + ignore_invalid
    ($name:expr, starts_with($prefix:expr), ignore_invalid) => {{
        use crate::transformer::builder::ColumnBuilder;
        use crate::transformer::validator::Validator;
        use polars::prelude::DataType;
        let mut col = ColumnBuilder::new($name, DataType::String);
        col.validators
            .push(Validator::TextStartsWith($prefix.into()));
        col.ignore_invalid_type()
    }};

    // mandatory + starts_with + ignore_invalid
    ($name:expr, mandatory, starts_with($prefix:expr), ignore_invalid) => {{
        use crate::transformer::builder::ColumnBuilder;
        use crate::transformer::validator::Validator;
        use polars::prelude::DataType;
        let mut col = ColumnBuilder::new($name, DataType::String);
        col.validators.push(Validator::NotNull);
        col.validators
            .push(Validator::TextStartsWith($prefix.into()));
        col.ignore_invalid_type()
    }};
}

#[macro_export]
macro_rules! int_col {
    // Caso base - apenas nome
    ($name:expr) => {{
        use crate::transformer::builder::ColumnBuilder;
        use polars::prelude::DataType;
        ColumnBuilder::new($name, DataType::Int64)
    }};

    // Apenas mandatory
    ($name:expr, mandatory) => {{
        use crate::transformer::builder::ColumnBuilder;
        use crate::transformer::validator::Validator;
        use polars::prelude::DataType;
        let mut col = ColumnBuilder::new($name, DataType::Int64);
        col.validators.push(Validator::NotNull);
        col
    }};

    // Apenas > valor
    ($name:expr, > $min:expr) => {{
        use crate::transformer::builder::ColumnBuilder;
        use crate::transformer::validator::Validator;
        use polars::prelude::DataType;
        let mut col = ColumnBuilder::new($name, DataType::Int64);
        col.validators.push(Validator::NumGreaterThan($min));
        col
    }};

    // Apenas ignore_invalid
    ($name:expr, ignore_invalid) => {{
        use crate::transformer::builder::ColumnBuilder;
        use polars::prelude::DataType;
        ColumnBuilder::new($name, DataType::Int64).ignore_invalid_type()
    }};

    // mandatory + > valor
    ($name:expr, mandatory, > $min:expr) => {{
        use crate::transformer::builder::ColumnBuilder;
        use crate::transformer::validator::Validator;
        use polars::prelude::DataType;
        let mut col = ColumnBuilder::new($name, DataType::Int64);
        col.validators.push(Validator::NotNull);
        col.validators.push(Validator::NumGreaterThan($min));
        col
    }};

    // mandatory + ignore_invalid
    ($name:expr, mandatory, ignore_invalid) => {{
        use crate::transformer::builder::ColumnBuilder;
        use crate::transformer::validator::Validator;
        use polars::prelude::DataType;
        let mut col = ColumnBuilder::new($name, DataType::Int64);
        col.validators.push(Validator::NotNull);
        col.ignore_invalid_type()
    }};

    // > valor + ignore_invalid
    ($name:expr, > $min:expr, ignore_invalid) => {{
        use crate::transformer::builder::ColumnBuilder;
        use crate::transformer::validator::Validator;
        use polars::prelude::DataType;
        let mut col = ColumnBuilder::new($name, DataType::Int64);
        col.validators.push(Validator::NumGreaterThan($min));
        col.ignore_invalid_type()
    }};

    // mandatory + > valor + ignore_invalid
    ($name:expr, mandatory, > $min:expr, ignore_invalid) => {{
        use crate::transformer::builder::ColumnBuilder;
        use crate::transformer::validator::Validator;
        use polars::prelude::DataType;
        let mut col = ColumnBuilder::new($name, DataType::Int64);
        col.validators.push(Validator::NotNull);
        col.validators.push(Validator::NumGreaterThan($min));
        col.ignore_invalid_type()
    }};
}

#[macro_export]
macro_rules! float_col {
    // Caso base - apenas nome
    ($name:expr) => {{
        use crate::transformer::builder::ColumnBuilder;
        use polars::prelude::DataType;
        ColumnBuilder::new($name, DataType::Float64)
    }};

    // Apenas mandatory
    ($name:expr, mandatory) => {{
        use crate::transformer::builder::ColumnBuilder;
        use crate::transformer::validator::Validator;
        use polars::prelude::DataType;
        let mut col = ColumnBuilder::new($name, DataType::Float64);
        col.validators.push(Validator::NotNull);
        col
    }};

    // Apenas > valor
    ($name:expr, > $min:expr) => {{
        use crate::transformer::builder::ColumnBuilder;
        use crate::transformer::validator::Validator;
        use polars::prelude::DataType;
        let mut col = ColumnBuilder::new($name, DataType::Float64);
        col.validators.push(Validator::NumGreaterThan($min));
        col
    }};

    // Apenas ignore_invalid
    ($name:expr, ignore_invalid) => {{
        use crate::transformer::builder::ColumnBuilder;
        use polars::prelude::DataType;
        ColumnBuilder::new($name, DataType::Float64).ignore_invalid_type()
    }};

    // mandatory + > valor
    ($name:expr, mandatory, > $min:expr) => {{
        use crate::transformer::builder::ColumnBuilder;
        use crate::transformer::validator::Validator;
        use polars::prelude::DataType;
        let mut col = ColumnBuilder::new($name, DataType::Float64);
        col.validators.push(Validator::NotNull);
        col.validators.push(Validator::NumGreaterThan($min));
        col
    }};

    // mandatory + ignore_invalid
    ($name:expr, mandatory, ignore_invalid) => {{
        use crate::transformer::builder::ColumnBuilder;
        use crate::transformer::validator::Validator;
        use polars::prelude::DataType;
        let mut col = ColumnBuilder::new($name, DataType::Float64);
        col.validators.push(Validator::NotNull);
        col.ignore_invalid_type()
    }};

    // > valor + ignore_invalid
    ($name:expr, > $min:expr, ignore_invalid) => {{
        use crate::transformer::builder::ColumnBuilder;
        use crate::transformer::validator::Validator;
        use polars::prelude::DataType;
        let mut col = ColumnBuilder::new($name, DataType::Float64);
        col.validators.push(Validator::NumGreaterThan($min));
        col.ignore_invalid_type()
    }};

    // mandatory + > valor + ignore_invalid
    ($name:expr, mandatory, > $min:expr, ignore_invalid) => {{
        use crate::transformer::builder::ColumnBuilder;
        use crate::transformer::validator::Validator;
        use polars::prelude::DataType;
        let mut col = ColumnBuilder::new($name, DataType::Float64);
        col.validators.push(Validator::NotNull);
        col.validators.push(Validator::NumGreaterThan($min));
        col.ignore_invalid_type()
    }};
}
