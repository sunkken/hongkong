-- testing/db_to_csv_tester_query.sql,
-- note that we are only matching hkex_main, not hkex_gem
SELECT gvkey, fa.isin, aof.document_name, aof.report_date, har.stock_code, listed_company_name, qualified_opinion, adverse_opinion, disclaimer_of_opinion, emphasis_of_matter, going_concern, datadate, conm, fyear
FROM auditor_opinion_flags AS aof
LEFT JOIN hkex_auditor_reports AS har
ON aof.document_name = har.document_name
LEFT JOIN hkex_main AS hm
	ON har.stock_code = hm.stock_code
LEFT JOIN funda_a_isin AS fa
	ON hm.isin = fa.isin
	AND strftime('%Y', aof.report_date) = strftime('%Y', fa.datadate)