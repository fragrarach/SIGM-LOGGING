CREATE OR REPLACE FUNCTION table_columns(IN CHARACTER)
  RETURNS TABLE(attname name, att_type text) AS
$BODY$ 
BEGIN
RETURN QUERY

WITH att_number AS (
        SELECT row_number() over() AS row_count, column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'public'
        AND table_name = '' || $1 || ''
)

SELECT pg_attribute.attname, format_type(atttypid, atttypmod) AS att_type
FROM pg_attribute
JOIN att_number ON column_name = pg_attribute.attname
WHERE pg_attribute.attname IN (
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'public'
        AND table_name = '' || $1 || ''
)
AND pg_attribute.attrelid = CAST ('' || $1 || '' AS regclass)
ORDER BY row_count

;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE

