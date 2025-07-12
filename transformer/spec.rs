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
            let expr = match spec.on_invalid {
                OnInvalid::Error => col(&spec.name).cast(spec.dtype.clone()),
                OnInvalid::Ignore => col(&spec.name).cast(spec.dtype.clone()).fill_null(lit(NULL)),
            };
            out = out.with_columns([expr]);
        }
        Ok(out)
    }

    fn validate_rows(&self, df: &DataFrame) -> Result<Vec<usize>, CsvTransformerError> {
        let mut valid = Vec::with_capacity(df.height());

        'rows: for row_idx in 0..df.height() {
            for col_spec in &self.cols {
                let val = df.column(&col_spec.name)?.get(row_idx)?;
                
                if !col_spec.validate(&val) {
                    match col_spec.on_invalid {
                        OnInvalid::Error => continue 'rows,
                        OnInvalid::Ignore => continue,
                    }
                }
            }
            valid.push(row_idx);
        }
        Ok(valid)
    }

    fn filter_valid_rows(&self, df: DataFrame, valid_indexes: Vec<usize>) -> Result<DataFrame, CsvTransformerError> {
        let indices_u32: Vec<u32> = valid_indexes.iter().map(|&i| i as u32).collect();
        let indices_chunked = UInt32Chunked::from_vec("valids".into(), indices_u32);
        
        df.take(&indices_chunked)
            .map_err(CsvTransformerError::PolarsError)
    }

    fn dataframe_to_records(
        &self,
        df: &DataFrame,
        orig_indexes: &[usize],
    ) -> Result<Vec<HashMap<String, Value>>, CsvTransformerError> {
        let mut records = Vec::with_capacity(df.height());

        for (pos, &orig_idx) in orig_indexes.iter().enumerate() {
            let mut record = HashMap::new();
            let mut skip_row = false;

            for col_spec in &self.cols {
                let val = df.column(&col_spec.name)?.get(pos)?;

                if val.is_null()
                    && matches!(
                        col_spec.dtype,
                        DataType::Int64 | DataType::Float64 | DataType::Int32 | DataType::Float32
                    )
                {
                    match col_spec.on_invalid {
                        OnInvalid::Error => {
                            return Err(CsvTransformerError::ValidacaoLinha {
                                linha: orig_idx + 2,
                                coluna: col_spec.name.clone(),
                                motivo: "valor não numérico encontrado em coluna que deveria ser numérica"
                                    .to_string(),
                            });
                        }
                        OnInvalid::Ignore => {
                            skip_row = true;
                            break;
                        }
                    }
                }

                record.insert(col_spec.name.clone(), anyvalue_to_json(&val)?);
            }
            if !skip_row {
                records.push(record);
            }
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
                        "Encontrado valor vazio/nulo em coluna que deveria ter um valor. Verifique se todas as colunas obrigatórias estão preenchidas.".to_string()
                    )
                } else if error_msg.contains("invalid type: string, expected") {
                    CsvTransformerError::ErroLeituraArquivo(
                        "Encontrado texto em coluna que deveria ser numérica. Verifique se os valores numéricos estão no formato correto.".to_string()
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

        let valid_indexes = self.validate_rows(&df)?;
        let filtered_df  = self.filter_valid_rows(df, valid_indexes.clone())?;
        let records      = self.dataframe_to_records(&filtered_df, &valid_indexes)?;

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