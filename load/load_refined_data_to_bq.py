from google.cloud import bigquery
import logging

def queries_to_bq_tables(path):
    
    client = bigquery.Client()
    with open(path) as file:
        query = file.read()

    job = client.query(query)
    job.result()

