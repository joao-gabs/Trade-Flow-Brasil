import pandas as pd
import logging

def run_data_quality_checks(df):

    logging.info("Iniciando Data Quality Checks")

    expected_columns = [
        "country",
        "heading",
        "economicBlock",
        "metricFOB",
        "metricKG",
        "flow",
        "hs_code",
        "ingestion_timestamp",
        "source"
    ]

    missing_cols = [col for col in expected_columns if col not in df.columns]

    if missing_cols:
        logging.error(f"Colunas faltando: {missing_cols}")
        raise Exception("Schema inválido")

    logging.info("Schema está validado!")

    critical_columns = ["flow", "hs_code"]

    for col in critical_columns:
        nulls = df[col].isnull().sum()

        if nulls > 0:
            logging.warning(f"{col} possui {nulls} valores nulos")

    duplicates = df.duplicated().sum()

    if duplicates > 0:
        logging.warning(f"{duplicates} registros duplicados encontrados")


    logging.info("Validação do tipo de dados...")

    df["metricFOB"] = pd.to_numeric(df["metricFOB"], errors="coerce")
    df["metricKG"] = pd.to_numeric(df["metricKG"], errors="coerce")


    logging.info("Data Quality finalizado")

    return df