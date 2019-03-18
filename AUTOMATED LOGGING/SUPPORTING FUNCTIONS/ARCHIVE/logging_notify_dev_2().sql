CREATE OR REPLACE FUNCTION logging_notify()
    RETURNS trigger AS
$BODY$
DECLARE
sigm_str    TEXT;
row_payload TEXT;
payload   TEXT;

BEGIN
sigm_str := (
    SELECT application_name 
    FROM pg_stat_activity 
    WHERE pid IN (
        SELECT pg_backend_pid()
    )
);
IF tg_op = 'DELETE' THEN

        row_payload := (
            '' || row_to_json(OLD)::text || ''
        );
        
ELSIF tg_op = 'INSERT' THEN

        row_payload := (
            '' || row_to_json(NEW)::text || ''
        );
        
ELSIF tg_op = 'UPDATE' THEN

    IF OLD <> NEW THEN
    
        row_payload := (
            '' || row_to_json(OLD)::text || ''
        );
        
    END IF;
END IF;

payload := (
    '' 
    || tg_table_name || ', '
    || tg_op || ', '
    || sigm_str || ', '
    || row_payload || ''
);

IF payload <> '' THEN

    IF (SELECT octet_length(payload::text) <= 8000) THEN
        PERFORM pg_notify(
            'logging', payload
        );
    ELSE
        PERFORM pg_notify(
            'logging',
            '' 
            || tg_table_name || ', '
            || tg_op || ', '
            || sigm_str || ''
        );
        
    END IF;
END IF;
RETURN NULL;
END;
$BODY$
    LANGUAGE plpgsql VOLATILE
    COST 100;
ALTER FUNCTION logging_notify()
    OWNER TO "SIGM";
