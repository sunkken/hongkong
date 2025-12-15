SELECT *
FROM funda_a_isin
INNER JOIN hkex_main ON funda_a_isin.isin = hkex_main.isin
GROUP BY gvkey;

-- Total unique ISINs in the funda_a_isin dataset
SELECT COUNT(DISTINCT isin) AS total_unique_isins
FROM funda_a_isin;

-- Total unique GVKEYs in the funda_a_isin dataset
SELECT COUNT(DISTINCT gvkey) AS total_unique_gvkeys
FROM funda_a_isin;

-- Unique ISINs that match a record in hkex_main
SELECT COUNT(DISTINCT funda_a_isin.isin) AS matched_unique_isins
FROM funda_a_isin
INNER JOIN hkex_main ON funda_a_isin.isin = hkex_main.isin;

-- Unique GVKEYs that match a record in hkex_main
SELECT COUNT(DISTINCT funda_a_isin.gvkey) AS matched_unique_gvkeys
FROM funda_a_isin
INNER JOIN hkex_main ON funda_a_isin.isin = hkex_main.isin;