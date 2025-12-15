-- Select rows from `hkex_dataset` but exclude stock_codes with no documents
-- Keeps the exact rows from hkex_dataset, filtered by existence of any matching document
SELECT hd.*
FROM hkex_dataset AS hd
WHERE EXISTS (
	SELECT 1
	FROM hkex_document_dataset AS doc
	WHERE doc.har_stock_code = hd.hkex_stock_code
);
