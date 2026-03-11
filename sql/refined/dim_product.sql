CREATE OR REPLACE TABLE trade_flow_brasil_refined.dim_product AS

SELECT DISTINCT
    hs_code,
    description,
    nomenclatura
    FROM trade_flow_brasil_curated.exports

UNION DISTINCT

SELECT DISTINCT
    hs_code,
    description,
    nomenclatura
    FROM trade_flow_brasil_curated.imports;