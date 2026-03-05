from google.cloud import storage
from io import BytesIO
from datetime import datetime
import logging


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