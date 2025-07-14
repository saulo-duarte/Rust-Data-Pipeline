use crate::transformer::errors::CsvTransformerError;
use crate::transformer::validator::Validator;
use polars::prelude::*;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug)]
pub struct ColSpec {
    pub name: String,
    pub dtype: DataType,
    pub validators: Vec<Validator>,
    pub on_invalid: OnInvalid,
}

#[derive(Debug, Clone, Copy)]
pub enum OnInvalid {
    Error,
    Ignore,
    Nullify,
}

impl ColSpec {
    pub fn validate(&self, val: &AnyValue) -> bool {
        self.validators.iter().all(|v| v.ok(val))
    }
}

#[derive(Debug)]
pub struct CsvTransformer {
    pub cols: Vec<ColSpec>,
}

impl CsvTransformer {
    fn validate_required_columns(&self, df: &DataFrame) -> Result<(), CsvTransformerError> {
        let missing_columns: Vec<String> = self.cols
            .iter()
            .filter_map(|col_spec| {
                if df.column(&col_spec.name).is_err() {
                    Some(col_spec.name.clone())
                } else {
                    None
                }
            })
            .collect();

        if !missing_columns.is_empty() {
            return Err(CsvTransformerError::ColunasFaltando(missing_columns));
        }

        Ok(())
    }

    pub fn apply_column_transformations(
        &self,
        lf: LazyFrame,
    ) -> polars::prelude::PolarsResult<LazyFrame> {
        let mut out = lf;
        for spec in &self.cols {
            // Aplica transformação básica de tipo
            let expr = col(&spec.name).cast(spec.dtype.clone());
            out = out.with_columns([expr]);
        }
        Ok(out)
    }

    fn validate_and_process_rows(&self, df: &DataFrame) -> Result<(DataFrame, Vec<usize>), CsvTransformerError> {
        let mut valid_rows = Vec::new();
        
        let mut columns_data: HashMap<String, Vec<AnyValue>> = HashMap::new();
        for spec in &self.cols {
            columns_data.insert(spec.name.clone(), Vec::new());
        }

        for row_idx in 0..df.height() {
            let mut row_data: HashMap<String, AnyValue> = HashMap::new();
            let mut skip_row = false;

            // Processa cada coluna da linha atual
            for col_spec in &self.cols {
                let original_val = df.column(&col_spec.name)?.get(row_idx)?;
                
                // Verifica se o valor é válido
                if !col_spec.validate(&original_val) {
                    match col_spec.on_invalid {
                        OnInvalid::Error => {
                            return Err(CsvTransformerError::ValidacaoLinha {
                                linha: row_idx + 2,
                                coluna: col_spec.name.clone(),
                                motivo: "valor não passou na validação".to_string(),
                            });
                        }
                        OnInvalid::Ignore => {
                            skip_row = true;
                            break;
                        }
                        OnInvalid::Nullify => {
                            row_data.insert(col_spec.name.clone(), AnyValue::Null);
                        }
                    }
                } else {
                    row_data.insert(col_spec.name.clone(), original_val);
                }
            }

            if !skip_row {
                valid_rows.push(row_idx);
                for col_spec in &self.cols {
                    let val = row_data.get(&col_spec.name).unwrap_or(&AnyValue::Null);
                    columns_data.get_mut(&col_spec.name).unwrap().push(val.clone());
                }
            }
        }

        let mut series_vec = Vec::new();
        for spec in &self.cols {
            let column_data = columns_data.get(&spec.name).unwrap();
            let series = Series::from_any_values(PlSmallStr::from(spec.name.as_str()), column_data, true)?;
            series_vec.push(series);
        }

        let columns: Vec<Column> = series_vec.into_iter()
            .map(|series| Column::new(series.name().clone(), series))
            .collect();

        let processed_df = DataFrame::new(columns)
            .map_err(CsvTransformerError::PolarsError)?;

        Ok((processed_df, valid_rows))
    }

    fn dataframe_to_records(
        &self,
        df: &DataFrame,
        orig_indexes: &[usize],
    ) -> Result<Vec<HashMap<String, Value>>, CsvTransformerError> {
        let mut records = Vec::with_capacity(df.height());

        for (pos, _) in orig_indexes.iter().enumerate() {
            let mut record = HashMap::new();

            for col_spec in &self.cols {
                let val = df.column(&col_spec.name)?.get(pos)?;

                record.insert(col_spec.name.clone(), anyvalue_to_json(&val)?);
            }
            
            records.push(record);
        }
        Ok(records)
    }

    fn records_to_typed_vec<T>(&self, records: Vec<HashMap<String, Value>>) -> Result<Vec<T>, CsvTransformerError>
    where
        T: DeserializeOwned,
    {
        let json_value = serde_json::to_value(records)
            .map_err(|e| CsvTransformerError::ErroLeituraArquivo(
                format!("Erro interno ao processar dados: {}", e)
            ))?;

        serde_json::from_value::<Vec<T>>(json_value)
            .map_err(|e| {
                let error_msg = e.to_string();
                
                if error_msg.contains("invalid type: null, expected") {
                    CsvTransformerError::ErroLeituraArquivo(
                        "Encontrado valor vazio/nulo em coluna que deveria ter um valor. Verifique se todas as colunas obrigatórias estão preenchidas ou configure OnInvalid::Nullify para permitir valores nulos.".to_string()
                    )
                } else if error_msg.contains("invalid type: string, expected") {
                    CsvTransformerError::ErroLeituraArquivo(
                        "Encontrado texto em coluna que deveria ser numérica. Verifique se os valores numéricos estão no formato correto ou configure OnInvalid::Nullify para permitir valores nulos.".to_string()
                    )
                } else {
                    CsvTransformerError::ErroLeituraArquivo(
                        format!("Erro ao processar dados: {}", error_msg)
                    )
                }
            })
    }

    pub fn extract<T>(&self, lf: LazyFrame) -> Result<Vec<T>, CsvTransformerError>
    where
        T: DeserializeOwned,
    {
        let lf = self.apply_column_transformations(lf)?;

        let df = lf.collect().map_err(|e| {
            let error_msg = e.to_string();
            
            if error_msg.contains("ColumnNotFound") {
                if let Some(start) = error_msg.find("ColumnNotFound(ErrString(\"") {
                    let start = start + "ColumnNotFound(ErrString(\"".len();
                    if let Some(end) = error_msg[start..].find("\\n") {
                        let column_name = &error_msg[start..start + end];
                        return CsvTransformerError::ColunasFaltando(vec![column_name.to_string()]);
                    }
                }
                CsvTransformerError::ColunasFaltando(vec!["coluna não identificada".to_string()])
            } else {
                CsvTransformerError::PolarsError(e)
            }
        })?;

        self.validate_required_columns(&df)?;

        let (processed_df, valid_indexes) = self.validate_and_process_rows(&df)?;
        
        let records = self.dataframe_to_records(&processed_df, &valid_indexes)?;

        self.records_to_typed_vec(records)
    }
}

fn anyvalue_to_json(val: &AnyValue) -> PolarsResult<Value> {
    let json_val = match val {
        AnyValue::Null => Value::Null,
        AnyValue::Boolean(b) => Value::Bool(*b),
        AnyValue::UInt8(n) => Value::Number((*n as u64).into()),
        AnyValue::UInt16(n) => Value::Number((*n as u64).into()),
        AnyValue::UInt32(n) => Value::Number((*n as u64).into()),
        AnyValue::UInt64(n) => Value::Number((*n).into()),
        AnyValue::Int8(n) => Value::Number((*n as i64).into()),
        AnyValue::Int16(n) => Value::Number((*n as i64).into()),
        AnyValue::Int32(n) => Value::Number((*n as i64).into()),
        AnyValue::Int64(n) => Value::Number((*n).into()),
        AnyValue::Float32(f) => serde_json::Number::from_f64(*f as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        AnyValue::Float64(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        AnyValue::String(s) => Value::String(s.to_string()),
        AnyValue::StringOwned(s) => Value::String(s.to_string()),
        _ => {
            return Err(PolarsError::ComputeError(
                format!("Tipo AnyValue não suportado: {:?}", val).into(),
            ))
        }
    };
    Ok(json_val)
}