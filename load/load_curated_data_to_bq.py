from google.cloud import bigquery
import logging


def load_to_bigquery(df, table_id):
    
    client = bigquery.Client()
    
    logging.info(f"Iniciando carga de {len(df)} registros para {table_id}")
    
    job_config = bigquery.LoadJobConfig(
        write_disposition = "WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED"
    )

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config
    )

    job.result()

    logging.info(f"Dados carregados com sucesso para {table_id}")
    logging.info(f"{len(df)} registros enviados para {table_id}")