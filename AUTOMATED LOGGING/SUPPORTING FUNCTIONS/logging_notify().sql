CREATE OR REPLACE FUNCTION logging_notify()
    RETURNS trigger AS
$BODY$
DECLARE
sigm_str    TEXT;
primary_key NAME;
pkey_ref    RECORD;
BEGIN
sigm_str := (
    SELECT application_name 
    FROM pg_stat_activity 
    WHERE pid IN (
        SELECT pg_backend_pid()
    )
);
primary_key := (
    SELECT * 
    FROM table_primary_keys('' || tg_table_name || '')
);
IF tg_op = 'DELETE' THEN
    EXECUTE format(
        'SELECT %s FROM %s WHERE %s = $1.%s', primary_key, tg_table_name, primary_key, primary_key
    )
    USING OLD
    INTO   pkey_ref;
ELSE
    EXECUTE format(
        'SELECT %s FROM %s WHERE %s = $1.%s', primary_key, tg_table_name, primary_key, primary_key
    )
    USING NEW
    INTO   pkey_ref;
END IF;
PERFORM pg_notify(
        'logging', '' 
        || tg_table_name || ', '
        || tg_op || ', '
        || primary_key || ', '
        || pkey_ref || ', '
        || sigm_str || ''
);
RETURN NULL;
END;
$BODY$
    LANGUAGE plpgsql VOLATILE
    COST 100;
ALTER FUNCTION logging_notify()
    OWNER TO "SIGM";
