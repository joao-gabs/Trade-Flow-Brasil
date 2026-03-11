CREATE OR REPLACE TABLE trade_flow_brasil_refined.dim_country AS

SELECT DISTINCT
    country,
    economic_block
FROM trade_flow_brasil_curated.exports

UNION DISTINCT

SELECT DISTINCT
    country,
    economic_block
FROM trade_flow_brasil_curated.imports;