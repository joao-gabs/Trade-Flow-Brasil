CREATE OR REPLACE TABLE trade_flow_brasil_refined.fact_trade_flow
PARTITION BY DATE(ingestion_timestamp)
CLUSTER BY country, hs_code
AS

SELECT * EXCEPT(row_num)
FROM (

SELECT *,
    ROW_NUMBER() OVER(
        PARTITION BY year, country, hs_code, flow
        ORDER BY ingestion_timestamp DESC
    ) AS row_num
FROM (

SELECT
    year,
    country,
    hs_code,
    flow,
    CAST(fob_value AS FLOAT64) AS fob_value,
    CAST(cif_value AS FLOAT64) AS cif_value,
    CAST(net_weight_kg AS FLOAT64) AS net_weight_kg,
    CAST(price_per_kg AS FLOAT64) AS price_per_kg,
    ingestion_timestamp
FROM trade_flow_brasil_curated.exports

UNION ALL

SELECT
    year,
    country,
    hs_code,
    flow,
    CAST(fob_value AS FLOAT64) AS fob_value,
    CAST(cif_value AS FLOAT64) AS cif_value,
    CAST(net_weight_kg AS FLOAT64) AS net_weight_kg,
    CAST(price_per_kg AS FLOAT64) AS price_per_kg,
    ingestion_timestamp
FROM trade_flow_brasil_curated.imports

)

)

WHERE row_num = 1