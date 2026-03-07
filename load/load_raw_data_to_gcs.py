from google.cloud import storage
from io import BytesIO
from datetime import datetime
import logging
import pandas as pd


def upload_df_to_gcs(df, bucket_name, folder):

    if df.empty:
        logging.critical("DataFrame vazio. Upload cancelado.")
        raise Exception("Tentativa de upload de DataFrame vazio")

    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        year = datetime.now().year

        blob_path = f"{folder}/{year}/{folder}_{timestamp}.parquet"

        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        blob = bucket.blob(blob_path)
        blob.upload_from_file(buffer)

        logging.info(f"Upload concluído: gs://{bucket_name}/{blob_path}")

    except Exception as e:
        logging.critical(f"Erro no upload para GCS: {e}")
        raise

def get_latest_blob(bucket_name, prefix):

    client = storage.Client()
    blobs = list(client.list_blobs(bucket_name, prefix=prefix))

    if not blobs:
        raise Exception(f"Nenhum arquivo encontrado em {prefix}")

    latest_blob = max(blobs, key=lambda blob: blob.updated)

    logging.info(f"Último arquivo encontrado: {latest_blob.name}")

    return latest_blob.name

def read_from_gcs(bucket_name, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    data = blob.download_as_bytes()

    df = pd.read_parquet(BytesIO(data))

    logging.info(f"Arquivo carregado do GCS: {blob_name}")
    logging.info(f"Shape: {df.shape}")
    logging.info(f"Colunas: {list(df.columns)}")
    logging.info(f"Tipos:\n{df.dtypes}")
    logging.info(f"Nulos:\n{df.isnull().sum()}")

    return df