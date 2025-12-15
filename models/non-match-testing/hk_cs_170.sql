SELECT *
FROM funda_a_170
INNER JOIN hkex_main ON funda_a_170.isin = hkex_main.isin
GROUP BY gvkey;
