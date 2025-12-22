SELECT isin FROM hkex_main WHERE stock_type = 'ORD SH'
UNION
SELECT isin FROM hkex_gem WHERE stock_type = 'ORD SH'
UNION
SELECT isin FROM hkex_isin WHERE stock_type = 'ORD SH'