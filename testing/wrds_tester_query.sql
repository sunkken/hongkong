SELECT gvkey, isin, datadate, fyear, at, revt, cstk
FROM comp.g_funda
WHERE exchg = 170
  AND indfmt = 'INDL'
  AND popsrc = 'I'
  AND consol = 'C'
  AND datafmt = 'HIST_STD'