SELECT gvkey, isin, datadate, fyearq, fqtr, atq, revtq, cstkq
FROM comp.g_fundq
WHERE exchg = 170
  AND indfmt = 'INDL'
  AND popsrc = 'I'
  AND consol = 'C'
  AND datafmt = 'HIST_STD'
