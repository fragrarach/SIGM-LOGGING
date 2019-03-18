CREATE OR REPLACE FUNCTION logging_notify()
    RETURNS trigger AS
$BODY$
DECLARE
sigm_str    TEXT;
primary_key NAME;
pkey_ref    RECORD;
short_load   TEXT;
long_load   TEXT;
BEGIN
sigm_str := (
    SELECT application_name 
    FROM pg_stat_activity 
    WHERE pid IN (
        SELECT pg_backend_pid()
    )
);
primary_key := (
    SELECT CASE WHEN EXISTS (
        SELECT pg_attribute.attname 
        FROM pg_index, pg_class, pg_attribute, pg_namespace 
        WHERE pg_class.oid = CAST ('' || tg_table_name || '' AS regclass)
        AND indrelid = pg_class.oid 
        AND nspname = 'public' 
        AND pg_class.relnamespace = pg_namespace.oid 
        AND pg_attribute.attrelid = pg_class.oid 
        AND pg_attribute.attnum = ANY(pg_index.indkey)
        AND indisprimary
    ) THEN (
        SELECT pg_attribute.attname 
        FROM pg_index, pg_class, pg_attribute, pg_namespace 
        WHERE pg_class.oid = CAST ('' || tg_table_name || '' AS regclass)
        AND indrelid = pg_class.oid 
        AND nspname = 'public' 
        AND pg_class.relnamespace = pg_namespace.oid 
        AND pg_attribute.attrelid = pg_class.oid 
        AND pg_attribute.attnum = ANY(pg_index.indkey)
        AND indisprimary
    ) ELSE 'no pkey'::TEXT END
);
IF tg_op = 'DELETE' THEN
    IF primary_key <> 'no pkey' THEN
        EXECUTE format(
            'SELECT %s FROM %s WHERE %s = $1.%s', primary_key, tg_table_name, primary_key, primary_key
        )
        USING OLD
        INTO   pkey_ref;
        long_load := (
            '' 
            || 'long_load' || ', '
            || tg_table_name || ', '
            || tg_op || ', '
            || sigm_str || ', '
            || primary_key || ', '
            || pkey_ref || ', '
            || row_to_json(OLD)::text || ''
        );
        short_load := (
            '' 
            || 'short_load' || ', '
            || tg_table_name || ', '
            || tg_op || ', '
            || sigm_str || ', '
            || primary_key || ', '
            || pkey_ref || ''
        );
    ELSE
        long_load := (
            '' 
            || 'medium_load' || ', '
            || tg_table_name || ', '
            || tg_op || ', '
            || sigm_str || ', '
            || row_to_json(OLD)::text || ''
        ); 
        short_load := (
            '' 
            || 'tiny_load' || ', '
            || tg_table_name || ', '
            || tg_op || ', '
            || sigm_str || ''
        );
    END IF;
ELSE
    IF primary_key <> 'no pkey' THEN
        EXECUTE format(
            'SELECT %s FROM %s WHERE %s = $1.%s', primary_key, tg_table_name, primary_key, primary_key
        )
        USING NEW
        INTO   pkey_ref;
        long_load := (
            '' 
            || 'long_load' || ', '
            || tg_table_name || ', '
            || tg_op || ', '
            || sigm_str || ', '
            || primary_key || ', '
            || pkey_ref || ', '
            || row_to_json(NEW)::text || ''
        );
        short_load := (
            '' 
            || 'short_load' || ', '
            || tg_table_name || ', '
            || tg_op || ', '
            || sigm_str || ', '
            || primary_key || ', '
            || pkey_ref || ''
        );
    ELSE
        long_load := (
            '' 
            || 'medium_load' || ', '
            || tg_table_name || ', '
            || tg_op || ', '
            || sigm_str || ', '
            || row_to_json(NEW)::text || ''
        ); 
        short_load := (
            '' 
            || 'tiny_load' || ', '
            || tg_table_name || ', '
            || tg_op || ', '
            || sigm_str || ''
        );
    END IF;
END IF;
IF (SELECT octet_length(long_load::text) > 8000) THEN
    PERFORM pg_notify(
        'logging', short_load
    );
ELSE
    PERFORM pg_notify(
        'logging', long_load
    );
END IF;
RETURN NULL;
END;
$BODY$
    LANGUAGE plpgsql VOLATILE
    COST 100;
ALTER FUNCTION logging_notify()
    OWNER TO "SIGM";
