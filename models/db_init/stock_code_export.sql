SELECT stock_code
FROM hkex_main
WHERE stock_type = 'ORD SH'
UNION
SELECT stock_code
FROM hkex_gem
WHERE stock_type = 'ORD SH';
