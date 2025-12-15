-- models/db_init/hkex_dataset.sql
-- Create a view that assembles auditor flags joined to HKEX stock info and WRDS funda_q_isin
DROP VIEW IF EXISTS hkex_dataset_bak;
CREATE VIEW hkex_dataset_bak AS
SELECT aof.document_name AS aof_document_id,
       aof.report_date AS aof_document_date,
       har.stock_code AS hkex_stock_code,
       hs.stock_type AS hkex_stock_type,
       listed_company_name AS hkex_full_name,
       qualified_opinion AS aof_qualified_op,
       adverse_opinion AS aof_adverse_op,
       disclaimer_of_opinion AS aof_disclaimer_op,
       emphasis_of_matter AS aof_emphasis_op,
       going_concern AS aof_material_unc_op,
       fq.gvkey AS cs_gvkey,
       fq.isin AS cs_isin,
       fq.conm AS cs_conm,
       fq.datadate AS cs_datadate,
       fq.datacqtr AS cs_datacqtr,
       fq.datafqtr AS cs_datafqtr,
       fq.fyearq AS cs_fyearq,
       fq.fqtr AS cs_fqtr
FROM auditor_opinion_flags AS aof
LEFT JOIN hkex_auditor_reports AS har
  ON aof.document_name = har.document_name
LEFT JOIN hkex_all_stock_code_isin AS hs
  ON har.stock_code = hs.stock_code
LEFT JOIN funda_q_isin AS fq
  ON hs.isin = fq.isin
  AND strftime('%Y', aof.report_date) = strftime('%Y', fq.datadate);

