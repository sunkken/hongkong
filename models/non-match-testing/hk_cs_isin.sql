SELECT *
FROM funda_a_isin
INNER JOIN hkex_main ON funda_a_isin.isin = hkex_main.isin
GROUP BY gvkey;