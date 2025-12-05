SELECT DISTINCT ar.stock_code, ar.pdf_path
FROM hkex_auditor_reports AS ar
INNER JOIN hkex_main AS m
  ON ar.stock_code = m.stock_code
WHERE ar.pdf_path IS NOT NULL
  AND TRIM(ar.pdf_path) <> ''
ORDER BY ar.stock_code;
