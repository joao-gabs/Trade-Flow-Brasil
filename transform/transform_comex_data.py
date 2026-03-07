import pandas as pd
import logging
import numpy as np

logging.basicConfig(level=logging.INFO,
                    format= "%(asctime)s | %(levelname)s | %(message)s")


def transform_data(df):

    logging.info("Iniciando transformações")

    df.rename(columns={'year': 'year', 'country': 'country','headingCode':'heading_code',
                       'heading': 'description', 'economicBlock':'economic_block', 'ncm':'nomenclatura','metricFOB':'fob_value',
                       'metricCIF':'cif_value', 'metricKG':'net_weight_kg','flow':'flow','hs_code':'hs_code',
                       'source':'source'}, inplace=True)

    df.columns = df.columns.str.lower()

    df = df.drop_duplicates(
    subset=["country", "flow", "hs_code"]
)

    df["price_per_kg"] = df["fob_value"] / df["net_weight_kg"].replace(0, pd.NA)

    if "cif_value" not in df.columns:
        df["cif_value"] = np.nan

    df = df.astype({'year': 'int', 'country': 'str', 'heading_code': 'int',
                    'description': 'str','economic_block':'str','nomenclatura': "str",'fob_value':'float',
                    'cif_value':'float','net_weight_kg':'float','flow':'str','hs_code':'int',
                    'source':'str'})

    logging.info("Transformação finalizada")

    return df
