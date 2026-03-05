import pandas as pd
import logging
from google.cloud import storage
from io import BytesIO
from data_quality_checks.data_quality import run_data_quality_checks
import re

logging.basicConfig(level=logging.INFO,
                    format= "%(asctime)s | %(levelname)s | %(message)s")

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


def transform_data(df):

    logging.info("Iniciando transformações")

    df.rename(columns={'year': 'year', 'country': 'country','headingCode':'heading_code',
                       'heading': 'description', 'economicBlock':'economic_block','metricFOB':'fob_value',
                       'metricCIF':'cif_value', 'metricKG':'net_weight_kg','flow':'flow','hs_code':'hs_code',
                       'source':'source'}, inplace=True)

    df.columns = df.columns.str.lower()

    df = df.drop_duplicates(
    subset=["country", "flow", "hs_code"]
)

    df["price_per_kg"] = df["fob_value"] / df["net_weight_kg"].replace(0, pd.NA)

    df = df.astype({'year': 'int', 'country': 'str', 'heading_code': 'int',
                    'description': 'str','economic_block':'str','fob_value':'float',
                    'cif_value':'float','net_weight_kg':'float','flow':'str','hs_code':'int',
                    'source':'str'})

    logging.info("Transformação finalizada")

    return df

df_parquet_imports = read_from_gcs(
    bucket_name="trade-flow-brasil-raw",
    blob_name="imports/2026/imports_20260305_113914.parquet"
)

df_validated = run_data_quality_checks(df_parquet_imports)

df_transformed = transform_data(df_validated)

print(df_transformed)
print(df_transformed.dtypes)
