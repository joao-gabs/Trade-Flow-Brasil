import logging
from extract.extract_data_from_comex import extract_comex_data
from load.load_raw_data_to_gcp import upload_df_to_gcs


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def run():

    bucket_name = "trade-flow-brasil-raw"

    try:
        logging.info("=== INÍCIO DO PIPELINE ===")

        # EXPORTS
        df_exports = extract_comex_data(
            headings=["2601", "1201", "1701", "0207", "4703"],
            flow="export",
            metrics=["metricFOB", "metricKG"],
            period_from="2025-01",
            period_to="2025-12"
        )

        upload_df_to_gcs(df_exports, bucket_name, "exports")

        # IMPORTS
        df_imports = extract_comex_data(
            headings=["8517", "3004", "3808", "3104"],
            flow="import",
            metrics=["metricFOB", "metricKG", "metricCIF"],
            period_from="2025-01",
            period_to="2025-12"
        )

        upload_df_to_gcs(df_imports, bucket_name, "imports")

        logging.info("=== PIPELINE FINALIZADO COM SUCESSO ===")

    except Exception as e:
        logging.critical(f"PIPELINE FALHOU: {e}")
        raise

if __name__ == "__main__":
    run()