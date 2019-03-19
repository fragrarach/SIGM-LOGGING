DROP TRIGGER IF EXISTS logging_notify ON part;
CREATE TRIGGER logging_notify
    AFTER UPDATE OR INSERT OR DELETE
    ON part
    FOR EACH ROW
    EXECUTE PROCEDURE logging_notify();

DROP TRIGGER IF EXISTS logging_notify ON order_header;
CREATE TRIGGER logging_notify
    AFTER UPDATE OR INSERT OR DELETE
    ON order_header
    FOR EACH ROW
    EXECUTE PROCEDURE logging_notify();