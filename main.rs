mod transformer;

use polars::prelude::*;
use serde::Serialize;
use std::thread;

#[derive(serde::Deserialize, Serialize, Debug)]
struct Client {
    id: i64,
    name: Option<String>,
    credit_limit: f64,
}

const PROJECT: &str = "teste-ctp";
const DATASET: &str = "financeiro";
const TABLE: &str = "clientes";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let clients = thread::spawn(|| -> PolarsResult<Vec<Client>> {
        let transformer = csv_schema![
            text_col!("name", mandatory),
            float_col!("credit_limit", mandatory),
            int_col!("id", mandatory, > 0.0),
        ];

        let lf = LazyCsvReader::new("clientes.csv")
            .with_has_header(true)
            .with_ignore_errors(true)
            .finish()?;

        let clients: Vec<Client> = transformer
            .extract(lf)
            .map_err(|e| PolarsError::ComputeError(format!("{:?}", e).into()))?;

        Ok(clients)
    })
    .join()
    .expect("thread panicked")?;

    println!("Linhas válidas: {}", clients.len());

    upload_to_bigquery(&clients)
        .await
        .expect("falha ao enviar para BigQuery");

    Ok(())
}

async fn upload_to_bigquery(rows: &[Client]) -> Result<(), Box<dyn std::error::Error>> {
    use reqwest::Client as Http;
    use serde_json::json;

    let provider = gcp_auth::provider().await?;
    let token = provider
        .token(&["https://www.googleapis.com/auth/bigquery"])
        .await?;
    let endpoint = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}/tables/{}/insertAll",
        PROJECT, DATASET, TABLE
    );

    let http = Http::new();

    const BATCH: usize = 10_000;
    for (chunk_idx, chunk) in rows.chunks(BATCH).enumerate() {
        let payload = json!({
            "rows": chunk.iter().enumerate().map(|(i, c)| {
                json!({
                    "insertId": format!("{}-{}", chunk_idx, i),
                    "json": c
                })
            }).collect::<Vec<_>>()
        });

        let resp = http
            .post(&endpoint)
            .bearer_auth(token.as_str())
            .json(&payload)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let txt = resp.text().await?;
            eprintln!("BigQuery erro HTTP {}: {}", status, txt);
            continue;
        }

        let body: serde_json::Value = resp.json().await?;
        if let Some(errs) = body.get("insertErrors") {
            eprintln!("Algumas linhas falharam: {}", errs);
        } else {
            println!("Batch {} enviado com sucesso!", chunk_idx);
        }
    }
    Ok(())
}
