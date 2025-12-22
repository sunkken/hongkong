-- Select all rows from the hkex_dataset view for CSV export
-- Only include rows where WRDS fiscal year (cs_fyearq) is 2014 or newer
SELECT *
FROM hkex_dataset
WHERE cs_fyearq >= 2014;
