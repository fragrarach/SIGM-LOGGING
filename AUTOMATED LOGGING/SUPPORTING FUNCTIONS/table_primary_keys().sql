CREATE OR REPLACE FUNCTION table_primary_keys(IN CHARACTER)
    RETURNS TABLE(attname name) AS
$BODY$ 
BEGIN
RETURN QUERY

SELECT pg_attribute.attname 
FROM pg_index, pg_class, pg_attribute, pg_namespace 
WHERE pg_class.oid = CAST ('' || $1 || '' AS regclass)
AND indrelid = pg_class.oid 
AND nspname = 'public' 
AND pg_class.relnamespace = pg_namespace.oid 
AND pg_attribute.attrelid = pg_class.oid 
AND pg_attribute.attnum = ANY(pg_index.indkey)
AND indisprimary;
END;
$BODY$
    LANGUAGE plpgsql VOLATILE

