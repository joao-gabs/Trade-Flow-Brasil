import logging
from extract.extract_data_from_comex import extract_comex_data
from load.load_raw_data_to_gcs import upload_df_to_gcs
from load.load_raw_data_to_gcs import get_latest_blob, read_from_gcs
from data_quality_checks.data_quality import run_data_quality_checks
from transform.transform_comex_data import transform_data
from load.load_curated_data_to_bq import load_to_bigquery
from load.load_refined_data_to_bq import queries_to_bq_tables

BUCKET_NAME = "trade-flow-brasil-raw"
TABLE_ID_EXPORTS = "pipeline-trade-flow-brazil.trade_flow_brasil_curated.exports"
TABLE_ID_IMPORTS = "pipeline-trade-flow-brazil.trade_flow_brasil_curated.imports"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

expected_columns_imports = [
    "year",
    "country",
    "heading",
    "economicBlock",
    "ncm",
    "metricFOB",
    "metricCIF",
    "metricKG",
    "flow",
    "hs_code",
    "ingestion_timestamp",
    "source"
]

expected_columns_exports = [
    "year",
    "country",
    "heading",
    "economicBlock",
    "ncm",
    "metricFOB",
    "metricKG",
    "flow",
    "hs_code",
    "ingestion_timestamp",
    "source"
]

def run():

    try:
        
        logging.info("=== INÍCIO DO PIPELINE ===")

        df_exports = extract_comex_data(
            headings=["2601", "1201", "1701", "0207", "4703"],
            flow="export",
            metrics=["metricFOB", "metricKG"],
            period_from="2015-01",
            period_to="2025-12"
        )

        upload_df_to_gcs(df_exports, BUCKET_NAME, "exports")

        df_imports = extract_comex_data(
            headings=["8517", "3004", "3808", "3104"],
            flow="import",
            metrics=["metricFOB", "metricKG", "metricCIF"],
            period_from="2015-01",
            period_to="2025-12"
        )

        upload_df_to_gcs(df_imports, BUCKET_NAME, "imports")

        latest_imports_blob = get_latest_blob(
            bucket_name= BUCKET_NAME,
            prefix="imports/2026/"
        )   

        latest_exports_blob = get_latest_blob(
            bucket_name= BUCKET_NAME,
            prefix="exports/2026/"
        )

        df_parquet_imports = read_from_gcs(
            bucket_name= BUCKET_NAME,
            blob_name= latest_imports_blob
        )

        df_parquet_exports = read_from_gcs(
            bucket_name =  BUCKET_NAME,
            blob_name = latest_exports_blob
        )

        df_imports_validated = run_data_quality_checks(
            df_parquet_imports,
            expected_columns = expected_columns_imports,
            numeric_columns=["metricFOB", "metricKG", "metricCIF"]
        )

        df_exports_validated = run_data_quality_checks(
            df_parquet_exports,
            expected_columns = expected_columns_exports,
            numeric_columns=["metricFOB", "metricKG"]
        )

        df_transformed_imports = transform_data(df_imports_validated)
        df_transformed_exports = transform_data(df_exports_validated)

        load_to_bigquery(
            df_transformed_exports,
            TABLE_ID_EXPORTS  
        )

        load_to_bigquery(
            df_transformed_imports,
            TABLE_ID_IMPORTS
        )   
        
        logging.info("Camada refined está em andamento...")
        queries_to_bq_tables("sql/refined/dim_country.sql")
        queries_to_bq_tables("sql/refined/dim_product.sql")
        queries_to_bq_tables("sql/refined/fact_trade_flow.sql")

        logging.info("=== PIPELINE FINALIZADO COM SUCESSO!")

    except Exception as e:
        logging.critical(f"PIPELINE FALHOU: {e}")
        raise

if __name__ == "__main__":
    run()