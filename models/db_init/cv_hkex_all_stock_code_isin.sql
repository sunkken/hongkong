-- Create a view that lists all stock codes with their ISINs
DROP VIEW IF EXISTS hkex_all_stock_code_isin;
CREATE VIEW hkex_all_stock_code_isin AS
SELECT stock_code, isin, MAX(company) AS company, stock_type
FROM (
    SELECT stock_code, isin, company, stock_type
    FROM hkex_main
    WHERE stock_type = 'ORD SH'
    UNION ALL
    SELECT stock_code, isin, company, stock_type
    FROM hkex_gem
    WHERE stock_type = 'ORD SH'
    UNION ALL
    SELECT stock_code, isin, company, stock_type
    FROM hkex_isin
    WHERE stock_type = 'ORD SH'
)
GROUP BY stock_code, isin, stock_type;