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
macro_rules! apply_col_options {
    ($col:ident, mandatory) => {
        $col.validators
            .push($crate::transformer::validator::Validator::NotNull);
    };
    ($col:ident, ignore_invalid) => {
        $col = $col.ignore_invalid_type();
    };
    ($col:ident, starts_with($prefix:expr)) => {
        $col.validators
            .push($crate::transformer::validator::Validator::TextStartsWith(
                $prefix.into(),
            ));
    };
    ($col:ident, > $min:expr) => {
        $col.validators
            .push($crate::transformer::validator::Validator::NumGreaterThan(
                $min,
            ));
    };
}

#[macro_export]
macro_rules! text_col {
    ($name:expr $(, $opt:tt )* $(,)?) => {{
        let mut col = $crate::transformer::builder::ColumnBuilder::new($name, polars::prelude::DataType::String);
        $(
            $crate::apply_col_options!(col, $opt);
        )*
        col
    }};
}

#[macro_export]
macro_rules! int_col {
    ($name:expr $(, $opt:tt )* $(,)?) => {{
        let mut col = $crate::transformer::builder::ColumnBuilder::new($name, polars::prelude::DataType::Int64);
        $(
            $crate::apply_col_options!(col, $opt);
        )*
        col
    }};
}

#[macro_export]
macro_rules! float_col {
    ($name:expr $(, $opt:tt )* $(,)?) => {{
        let mut col = $crate::transformer::builder::ColumnBuilder::new($name, polars::prelude::DataType::Float64);
        $(
            $crate::apply_col_options!(col, $opt);
        )*
        col
    }};
}
