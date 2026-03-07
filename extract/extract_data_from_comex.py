import requests
import time
import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

MAX_RETRIES = 5

def make_request_with_retry(url, headers, payload):

    for attempt in range(MAX_RETRIES):

        try:    
            response = requests.post(url, headers=headers, json=payload)

            if response.status_code == 429:
                wait_time = 2 ** attempt
                logging.warning(
                    f"Rate limit atingido. Tentativa {attempt+1}. "
                    f"Aguardando {wait_time}s..."
                )
                time.sleep(wait_time)
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logging.error(f"Erro na requisição: {e}")

            if attempt == MAX_RETRIES - 1:
                logging.critical("Máximo de tentativas atingido. Encerrando pipeline.")
                return None

            wait_time = 2 ** attempt    
            time.sleep(wait_time)
    
    return None

def extract_comex_data(headings, flow, metrics, period_from, period_to):

    url = "https://api-comexstat.mdic.gov.br/general"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    all_data = []

    for heading in headings:

        logging.info(f"Iniciando extração | Flow: {flow} | HS: {heading}")

        payload = {
            "flow": flow,
            "monthDetail": False,
            "period": {
                "from": period_from,
                "to": period_to
            },
            "filters": [
                {
                    "filter": "heading",
                    "values": [heading]
                }
            ],
            "details": ["country", "heading", "economicBlock", "ncm"],
            "metrics": metrics
        }

        data = make_request_with_retry(url, headers, payload)

        if data is None:
            logging.error(f"Resposta vazia da API para HS {heading}")
            raise Exception("Falha na extração de dados da API!")

        if not data.get("success"):
            logging.error(f"API retornou sucesso=False para HS {heading}")
            continue

        records = data["data"]["list"]

        for record in records:
            record["flow"] = flow
            record["hs_code"] = heading

        all_data.extend(records)

        logging.info(f"Registros coletados: {len(records)}")

        time.sleep(5)

    df = pd.DataFrame(all_data)
    df["ingestion_timestamp"] = datetime.utcnow()
    df["source"] = "comexstat_api"

    if df.empty:
        logging.critical("DataFrame vazio. Abortando pipeline.")
        raise Exception("Nenhum dado coletado")

    logging.info(f"Extração finalizada. Total de registros: {len(df)}")

    return df