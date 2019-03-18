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

IF tg_table_name = 'part' THEN

    IF 
        tg_op = 'UPDATE'
    THEN
        IF
            OLD.prt_no <> NEW.prt_no
        THEN
            row_payload := (
                ''
                || 'RENAMED PART' || ', '
                || '{' || ''
                || 'old_prt_no:' || ''
                || OLD.prt_no::text || ', '
                || 'new_prt_no:' || ''
                || NEW.prt_no::text || ''
                || '}'
            );
        END IF;
    END IF;
        
ELSIF tg_table_name = 'part_price' THEN

    IF 
        tg_op = 'UPDATE'
    THEN
        IF
            OLD.ppr_price <> NEW.ppr_price
        THEN
            row_payload := (
                ''
                || 'PART PRICE CHANGED' || ', '
                || '{' || ''
                || 'prt_id:' || ''
                || NEW.prt_id::text || ', '
                || 'price_level:' || ''
                || NEW.ppr_sort_idx::text || ', '
                || 'old_price:' || ''
                || OLD.ppr_price::text || ', '
                || 'new_price:' || ''
                || NEW.ppr_price::text || ''

                || '}'
            );
        END IF;
    END IF;

ELSIF tg_table_name = 'part_supplier' THEN

    IF 
        tg_op = 'INSERT'
    THEN
        -- IF
            -- OLD.psp_price <> NEW.psp_price
        -- THEN
            row_payload := (
                ''
                || 'PART COST CHANGED' || ', '
                || '{' || ''
                || 'prt_id:' || ''
                || NEW.prt_id::text || ', '
                || 'sup_id:' || ''
                || NEW.sup_id::text || ', '
                || 'psp_price:' || ''
                || NEW.psp_price::text || ''
                || '}'
            );
        -- END IF;
    END IF;

ELSIF tg_table_name = 'invoicing' THEN

    IF 
        tg_op = 'UPDATE'
    THEN
        IF
            OLD.inv_no = 0 
            AND NEW.inv_no <> 0
        THEN
            row_payload := (
                ''
                || 'PACKING SLIP DATE' || ', '
                || '{' || ''
                || 'inv_id:' || ''
                || NEW.inv_id::text || ', '
                || 'packing_slip_date:' || ''
                || OLD.inv_date::text || ''
                || '}'
            );
        END IF;
    END IF;

-- ELSIF tg_table_name = 'bill_of_materials_mat' THEN

-- ELSIF tg_table_name = 'part_kit' THEN

-- ELSIF tg_table_name = 'contract' THEN

-- ELSIF tg_table_name = 'contract_group_line' THEN

-- ELSIF tg_table_name = 'contract_part_line' THEN

END IF;

IF row_payload <> '' THEN
    payload := (
        '' 
        || sigm_str || ', '
        || row_payload || ''
    );
END IF;

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
