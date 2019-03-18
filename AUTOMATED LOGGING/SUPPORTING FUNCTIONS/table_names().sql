CREATE OR REPLACE FUNCTION table_names()
  RETURNS TABLE(table_name VARCHAR) AS
$BODY$ 
BEGIN
RETURN QUERY
    SELECT tables.table_name::VARCHAR
    FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_type = 'BASE TABLE'
;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
  ROWS 1000;
ALTER FUNCTION table_names()
  OWNER TO "SIGM";