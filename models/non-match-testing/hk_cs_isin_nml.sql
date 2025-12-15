-- Funda_a ISIN Rows with no isin-match in Hkex
SELECT funda_a_isin.*
FROM funda_a_isin
LEFT JOIN hkex_main
    ON funda_a_isin.isin = hkex_main.isin
WHERE hkex_main.isin IS NULL;

-- Unique GVKEY & ISIN from Funda_a_ISIN rows with no ISIN match in Hkex
SELECT DISTINCT funda_a_isin.gvkey, funda_a_isin.isin
FROM funda_a_isin
LEFT JOIN hkex_main
    ON funda_a_isin.isin = hkex_main.isin
WHERE hkex_main.isin IS NULL;

-- Count unique ISINs missing on left
SELECT COUNT(DISTINCT funda_a_isin.isin) AS missing_isins
FROM funda_a_isin
LEFT JOIN hkex_main
    ON funda_a_isin.isin = hkex_main.isin
WHERE hkex_main.isin IS NULL;

-- Count unique GVKEYs missing on left
SELECT COUNT(DISTINCT funda_a_isin.gvkey) AS missing_gvkeys
FROM funda_a_isin
LEFT JOIN hkex_main
    ON funda_a_isin.isin = hkex_main.isin
WHERE hkex_main.isin IS NULL;