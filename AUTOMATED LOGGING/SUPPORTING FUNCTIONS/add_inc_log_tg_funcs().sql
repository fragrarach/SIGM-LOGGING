CREATE OR REPLACE FUNCTION add_inc_log_tg_funcs(IN CHARACTER)
  RETURNS void AS
$BODY$ 
BEGIN

EXECUTE
'DROP TRIGGER IF EXISTS logging_notify ON ' || $1 || ';
CREATE TRIGGER logging_notify
    AFTER UPDATE OR INSERT OR DELETE
    ON ' || $1 ||
    ' FOR EACH ROW
    EXECUTE PROCEDURE logging_notify()'
    
;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION add_inc_log_tg_funcs(IN CHARACTER)
  OWNER TO "SIGM";