-- Hkex rows with no ISIN match in Funda_a_ex170
SELECT hkex_main.*
FROM hkex_main
LEFT JOIN funda_a_170
    ON hkex_main.isin = funda_a_170.isin
WHERE funda_a_170.isin IS NULL;

-- Unique ISINs from Hkex rows with no ISIN match in Funda_a
SELECT DISTINCT hkex_main.isin
FROM hkex_main
LEFT JOIN funda_a_170
    ON hkex_main.isin = funda_a_170.isin
WHERE funda_a_170.isin IS NULL;

-- Count unique ISINs missing on right
SELECT COUNT(DISTINCT hkex_main.isin) AS missing_isins
FROM hkex_main
LEFT JOIN funda_a_170
    ON hkex_main.isin = funda_a_170.isin
WHERE funda_a_170.isin IS NULL;