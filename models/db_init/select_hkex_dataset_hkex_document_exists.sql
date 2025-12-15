-- Select rows from `hkex_dataset` but exclude stock_codes with no documents
-- Keeps the exact rows from hkex_dataset, filtered by existence of any matching document
SELECT hd.*
FROM hkex_dataset AS hd
WHERE EXISTS (
	SELECT 1
	FROM hkex_document_dataset AS doc
	WHERE doc.har_stock_code = hd.hkex_stock_code
	  -- Only consider documents from 2014 onwards when testing existence
	  AND (
	    (doc.har_announcement_date IS NOT NULL AND doc.har_announcement_date >= '2014-01-01')
	    OR (doc.aof_report_date IS NOT NULL AND doc.aof_report_date >= '2014-01-01')
	  )
	  -- Also require the hkex_dataset fiscal year to be 2014 or newer
	  AND hd.cs_fyearq >= 2014
);
