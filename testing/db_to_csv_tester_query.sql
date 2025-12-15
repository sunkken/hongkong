-- models/db_init/db_to_csv_tester_query_hkex_view.sql
-- Variant of testing/db_to_csv_tester_query.sql that joins to the hkex_all_stock_code_isin view
SELECT gvkey, fa.isin, aof.document_name, aof.report_date, har.stock_code, hs.stock_type, listed_company_name, qualified_opinion, adverse_opinion, disclaimer_of_opinion, emphasis_of_matter, going_concern, datadate, conm, fyear
FROM auditor_opinion_flags AS aof
LEFT JOIN hkex_auditor_reports AS har
  ON aof.document_name = har.document_name
LEFT JOIN hkex_all_stock_code_isin AS hs
  ON har.stock_code = hs.stock_code
LEFT JOIN funda_a_isin AS fa
  ON hs.isin = fa.isin
  AND strftime('%Y', aof.report_date) = strftime('%Y', fa.datadate)
