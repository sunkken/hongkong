-- Select all rows from the hkex_document_dataset view for CSV export
-- Only include documents from 2014-01-01 onwards
SELECT *
FROM hkex_document_dataset
WHERE aof_report_date >= '2014-01-01';
