mod transformer;

use polars::prelude::*;
use serde::Serialize;
use std::thread;

#[derive(serde::Deserialize, Serialize, Debug)]
struct Client {
    id: Option<i64>,
    name: Option<String>,
    credit_limit: Option<f64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let clients = thread::spawn(|| -> PolarsResult<Vec<Client>> {
        let transformer = csv_schema![
            text_col!("name").nullify_invalid(),
            float_col!("credit_limit", mandatory, ignore_invalid).nullify_invalid(),
            int_col!("id", mandatory, ignore_invalid).nullify_invalid(),
        ];

        let fields = vec![
            Field::new(PlSmallStr::from("id"), DataType::Int64),
            Field::new(PlSmallStr::from("name"), DataType::String),
            Field::new(PlSmallStr::from("credit_limit"), DataType::Float64),
        ];

        let schema = Schema::from_iter_check_duplicates(fields.into_iter()).expect("schema erro");

        let lf = LazyCsvReader::new("clientes.csv")
            .with_schema(Some(Arc::new(schema)))
            .with_has_header(true)
            .with_separator(b';')
            .with_ignore_errors(true)
            .with_decimal_comma(true)
            .finish()?;

        let clients: Vec<Client> = transformer
            .extract(lf)
            .map_err(|e| PolarsError::ComputeError(format!("{:?}", e).into()))?;

        Ok(clients)
    })
    .join()
    .expect("thread panicked")?;

    println!("Linhas válidas: {:?}", clients);

    Ok(())
}
