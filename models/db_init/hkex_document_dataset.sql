-- models/db_init/hkex_dataset.sql
-- Create a view that assembles auditor flags joined to HKEX stock info and WRDS funda_q_isin
DROP VIEW IF EXISTS hkex_document_dataset;
CREATE VIEW hkex_document_dataset AS
SELECT har.stock_code AS har_stock_code,
       har.announcement_date AS har_announcement_date,
       har.hyperlink AS har_hyperlink,
       har.pdf_path AS har_pdf_path,
       hs.stock_type AS hkex_stock_type,
       hs.company AS hkex_full_name,
       aof.document_name AS aof_document_id,
       aof.report_date AS aof_report_date,
       qualified_opinion AS aof_qualified_op,
       adverse_opinion AS aof_adverse_op,
       disclaimer_of_opinion AS aof_disclaimer_op,
       emphasis_of_matter AS aof_emphasis_op,
       going_concern AS aof_material_unc_op
FROM auditor_opinion_flags AS aof
LEFT JOIN hkex_auditor_reports AS har
  ON aof.document_name = har.document_name
LEFT JOIN hkex_all_stock_code_isin AS hs
  ON har.stock_code = hs.stock_code;

