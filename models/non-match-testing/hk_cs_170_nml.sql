-- Funda_a ex170 Rows with no isin-match in Hkex
SELECT funda_a_170.*
FROM funda_a_170
LEFT JOIN hkex_main
    ON funda_a_170.isin = hkex_main.isin
WHERE hkex_main.isin IS NULL;

-- Unique GVKEY & ISIN from Funda_a rows with no ISIN match in Hkex
SELECT DISTINCT funda_a_170.gvkey, funda_a_170.isin
FROM funda_a_170
LEFT JOIN hkex_main
    ON funda_a_170.isin = hkex_main.isin
WHERE hkex_main.isin IS NULL;

-- Count unique ISINs missing on left
SELECT COUNT(DISTINCT funda_a_170.isin) AS missing_isins
FROM funda_a_170
LEFT JOIN hkex_main
    ON funda_a_170.isin = hkex_main.isin
WHERE hkex_main.isin IS NULL;

-- Count unique GVKEYs missing on left
SELECT COUNT(DISTINCT funda_a_170.gvkey) AS missing_gvkeys
FROM funda_a_170
LEFT JOIN hkex_main
    ON funda_a_170.isin = hkex_main.isin
WHERE hkex_main.isin IS NULL;
