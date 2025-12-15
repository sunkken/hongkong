-- Create a view that lists all stock codes with their ISINs
DROP VIEW IF EXISTS hkex_all_stock_code_isin;
CREATE VIEW hkex_all_stock_code_isin AS
SELECT stock_code, isin, company, stock_type
FROM hkex_main
UNION
SELECT stock_code, isin, company, stock_type
FROM hkex_gem;