SELECT gvkey, isin, conm, datadate, fyear
FROM comp.g_funda
WHERE exchg = 170
  AND indfmt = 'INDL'
  AND popsrc = 'I'
  AND consol = 'C'
  AND datafmt = 'HIST_STD'
