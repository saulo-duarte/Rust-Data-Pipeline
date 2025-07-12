use thiserror::Error;

#[derive(Error, Debug)]
pub enum CsvTransformerError {
    #[error("Erro na operação com Polars: {0}")]
    PolarsError(#[from] polars::prelude::PolarsError),

    #[error("Erro na leitura do arquivo: {0}")]
    ErroLeituraArquivo(String),

    #[error("O Arquivo está faltando as colunas obrigatórias: {0:?}")]
    ColunasFaltando(Vec<String>),

    #[error("Linha {linha} inválida na coluna '{coluna}': {motivo}")]
    ValidacaoLinha {
        linha: usize,
        coluna: String,
        motivo: String,
    },
}
