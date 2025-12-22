-- Create a view of hkex_all_stock_code_isin rows without matching ISIN in funda_q_170
DROP VIEW IF EXISTS non_match_hkex_isin;
CREATE VIEW non_match_hkex_isin AS
SELECT DISTINCT h.stock_code, h.isin, h.company, h.stock_type
FROM hkex_all_stock_code_isin h
LEFT JOIN funda_q_170 f ON h.isin = f.isin
WHERE f.isin IS NULL;