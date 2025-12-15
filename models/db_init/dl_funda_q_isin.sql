SELECT gvkey, isin, conm, datadate, datacqtr, datafqtr, fyearq, fqtr
FROM comp.g_fundq
WHERE indfmt = 'INDL'
  AND popsrc = 'I'
  AND consol = 'C'
  AND datafmt = 'HIST_STD'