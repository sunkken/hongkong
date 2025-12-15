-- models/db_init/hkex_dataset.sql
-- Create a view that assembles funda_q_isin joined to HKEX stock info
DROP VIEW IF EXISTS hkex_dataset;
CREATE VIEW hkex_dataset AS
SELECT 
       hs.stock_code AS hkex_stock_code,
       hs.stock_type AS hkex_stock_type,
       hs.company AS hkex_full_name,
       fq.gvkey AS cs_gvkey,
       fq.isin AS cs_isin,
       fq.conm AS cs_conm,
       fq.datadate AS cs_datadate,
       fq.datacqtr AS cs_datacqtr,
       fq.datafqtr AS cs_datafqtr,
       fq.fyearq AS cs_fyearq,
       fq.fqtr AS cs_fqtr
FROM funda_q_isin AS fq
LEFT JOIN hkex_all_stock_code_isin AS hs
  ON fq.isin = hs.isin