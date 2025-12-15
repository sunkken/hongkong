SELECT *
FROM funda_a_170
INNER JOIN hkex_main ON funda_a_170.isin = hkex_main.isin
GROUP BY gvkey;

-- Total unique ISINs in the funda_a_170 dataset
SELECT COUNT(DISTINCT isin) AS total_unique_isins
FROM funda_a_170;

-- Total unique GVKEYs in the funda_a_170 dataset
SELECT COUNT(DISTINCT gvkey) AS total_unique_gvkeys
FROM funda_a_170;

-- Unique ISINs that match a record in hkex_main
SELECT COUNT(DISTINCT funda_a_170.isin) AS matched_unique_isins
FROM funda_a_170
INNER JOIN hkex_main ON funda_a_170.isin = hkex_main.isin;

-- Unique GVKEYs that match a record in hkex_main
SELECT COUNT(DISTINCT funda_a_170.gvkey) AS matched_unique_gvkeys
FROM funda_a_170
INNER JOIN hkex_main ON funda_a_170.isin = hkex_main.isin;
