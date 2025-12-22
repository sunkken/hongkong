-- Create a view of funda_q_170 rows without matching ISIN in hkex_all_stock_code_isin
DROP VIEW IF EXISTS non_match_funda_q_170;
CREATE VIEW non_match_funda_q_170 AS
SELECT DISTINCT f.gvkey, f.isin, f.conm
FROM funda_q_170 f
LEFT JOIN hkex_all_stock_code_isin h ON f.isin = h.isin
WHERE h.isin IS NULL;