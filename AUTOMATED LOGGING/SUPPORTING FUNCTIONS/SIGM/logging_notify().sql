CREATE OR REPLACE FUNCTION logging_notify()
    RETURNS trigger AS
$BODY$
DECLARE
sigm_str    TEXT;
row_payload TEXT;
old_payload TEXT;
new_payload TEXT;
payload   TEXT;

BEGIN

sigm_str := (
    SELECT application_name
    FROM pg_stat_activity
    WHERE pid IN (
        SELECT pg_backend_pid()
    )
);

IF
    tg_op = 'UPDATE'
THEN

    IF
        tg_table_name = 'order_header'
    THEN

        IF
            NEW.prj_no IS NULL AND
            NEW.ord_id <> OLD.ord_id OR
            NEW.orl_first_id <> OLD.orl_first_id OR
            NEW.ord_line_nb <> OLD.ord_line_nb OR
            NEW.ord_date <> OLD.ord_date OR
            NEW.cli_id <> OLD.cli_id OR
            NEW.ord_no <> OLD.ord_no OR
            NEW.inv_cli_id <> OLD.inv_cli_id OR
            NEW.ord_finv_to_cli <> OLD.ord_finv_to_cli OR
            NEW.ord_notes_stat <> OLD.ord_notes_stat OR
            NEW.ord_note1 <> OLD.ord_note1 OR
            NEW.ord_note2 <> OLD.ord_note2 OR
            NEW.ord_note3 <> OLD.ord_note3 OR
            NEW.ord_note4 <> OLD.ord_note4 OR
            NEW.cli_inv_note1 <> OLD.cli_inv_note1 OR
            NEW.cli_inv_note2 <> OLD.cli_inv_note2 OR
            NEW.ord_pckslp_no1 <> OLD.ord_pckslp_no1 OR
            NEW.ord_pckslp_no2 <> OLD.ord_pckslp_no2 OR
            NEW.ord_pckslp_no3 <> OLD.ord_pckslp_no3 OR
            NEW.ord_pckslp_no4 <> OLD.ord_pckslp_no4 OR
            NEW.ord_ship_dt1 <> OLD.ord_ship_dt1 OR
            NEW.ord_ship_dt2 <> OLD.ord_ship_dt2 OR
            NEW.ord_ship_dt3 <> OLD.ord_ship_dt3 OR
            NEW.ord_ship_dt4 <> OLD.ord_ship_dt4 OR
            NEW.ord_inv_no1 <> OLD.ord_inv_no1 OR
            NEW.ord_inv_no2 <> OLD.ord_inv_no2 OR
            NEW.ord_inv_no3 <> OLD.ord_inv_no3 OR
            NEW.ord_inv_no4 <> OLD.ord_inv_no4 OR
            NEW.ord_status <> OLD.ord_status OR
            NEW.ord_req_dt <> OLD.ord_req_dt OR
            NEW.ord_cancel_dt <> OLD.ord_cancel_dt OR
            NEW.ord_pmt_term <> OLD.ord_pmt_term OR
            NEW.ord_fpmt_term <> OLD.ord_fpmt_term OR
            NEW.ord_fcli_dscnt <> OLD.ord_fcli_dscnt OR
            NEW.ord_ship_term <> OLD.ord_ship_term OR
            NEW.ord_fship_term <> OLD.ord_fship_term OR
            NEW.car_name <> OLD.car_name OR
            NEW.car_fname <> OLD.car_fname OR
            NEW.ter_id <> OLD.ter_id OR
            NEW.ord_fter_id <> OLD.ord_fter_id OR
            NEW.ord_fship <> OLD.ord_fship OR
            NEW.sal_id <> OLD.sal_id OR
            NEW.ord_fsal_id <> OLD.ord_fsal_id OR
            NEW.ord_fsal_comm <> OLD.ord_fsal_comm OR
            NEW.cli_del_name1 <> OLD.cli_del_name1 OR
            NEW.cli_del_name2 <> OLD.cli_del_name2 OR
            NEW.cli_del_addr <> OLD.cli_del_addr OR
            NEW.cli_del_city <> OLD.cli_del_city OR
            NEW.cli_del_pc <> OLD.cli_del_pc OR
            NEW.cli_phone1 <> OLD.cli_phone1 OR
            NEW.ord_fdel <> OLD.ord_fdel OR
            NEW.inv_name1 <> OLD.inv_name1 OR
            NEW.inv_name2 <> OLD.inv_name2 OR
            NEW.inv_addr <> OLD.inv_addr OR
            NEW.inv_city <> OLD.inv_city OR
            NEW.inv_pc <> OLD.inv_pc OR
            NEW.inv_phone <> OLD.inv_phone OR
            NEW.ord_bo_accptd <> OLD.ord_bo_accptd OR
            NEW.ord_pckslp_stat1 <> OLD.ord_pckslp_stat1 OR
            NEW.ord_pckslp_stat2 <> OLD.ord_pckslp_stat2 OR
            NEW.ord_pckslp_stat3 <> OLD.ord_pckslp_stat3 OR
            NEW.ord_pckslp_stat4 <> OLD.ord_pckslp_stat4 OR
            NEW.cli_dscnt_fixed <> OLD.cli_dscnt_fixed OR
            NEW.sal_comm_fixed <> OLD.sal_comm_fixed OR
            NEW.ord_cli_ord_no <> OLD.ord_cli_ord_no OR
            NEW.ord_use_ship_amnt <> OLD.ord_use_ship_amnt OR
            NEW.inv_pckslp_prnfmt <> OLD.inv_pckslp_prnfmt OR
            NEW.inv_prnfmt <> OLD.inv_prnfmt OR
            NEW.ord_export <> OLD.ord_export OR
            -- NEW.prj_no <> OLD.prj_no OR
            NEW.cod_no <> OLD.cod_no OR
            NEW.ord_ftax <> OLD.ord_ftax OR
            NEW.ord_finv_addr <> OLD.ord_finv_addr OR
            NEW.ord_inv_onhold <> OLD.ord_inv_onhold OR
            NEW.ord_prj_add_class <> OLD.ord_prj_add_class OR
            -- NEW.ord_prj_add_no <> OLD.ord_prj_add_no OR
            NEW.ord_prnfmt <> OLD.ord_prnfmt OR
            NEW.acm_id <> OLD.acm_id OR
            NEW.inv_del_fax <> OLD.inv_del_fax OR
            NEW.inv_fax <> OLD.inv_fax OR
            NEW.ord_imported <> OLD.ord_imported OR
            NEW.whs_no <> OLD.whs_no OR
            NEW.ord_expir_dt <> OLD.ord_expir_dt OR
            NEW.ord_orig_req_dt <> OLD.ord_orig_req_dt OR
            NEW.ord_req_time <> OLD.ord_req_time OR
            NEW.ord_oreq_time <> OLD.ord_oreq_time OR
            NEW.cli_taxe_group_no <> OLD.cli_taxe_group_no OR
            NEW.ord_taxe_1_no <> OLD.ord_taxe_1_no OR
            NEW.cli_exempt1_no <> OLD.cli_exempt1_no OR
            NEW.ord_taxe_2_no <> OLD.ord_taxe_2_no OR
            NEW.cli_exempt2_no <> OLD.cli_exempt2_no OR
            NEW.ord_taxe_3_no <> OLD.ord_taxe_3_no OR
            NEW.cli_exempt3_no <> OLD.cli_exempt3_no OR
            NEW.ord_taxe_4_no <> OLD.ord_taxe_4_no OR
            NEW.cli_exempt4_no <> OLD.cli_exempt4_no OR
            NEW.ord_taxe_5_no <> OLD.ord_taxe_5_no OR
            NEW.cli_exempt5_no <> OLD.cli_exempt5_no OR
            NEW.cli_dscnt <> OLD.cli_dscnt OR
            NEW.ord_ship_rate <> OLD.ord_ship_rate OR
            NEW.sal_comm <> OLD.sal_comm OR
            NEW.ord_pship_rate <> OLD.ord_pship_rate OR
            NEW.ord_tship_rate <> OLD.ord_tship_rate OR
            NEW.ord_ship_amnt <> OLD.ord_ship_amnt OR
            NEW.ord_pkg_cost <> OLD.ord_pkg_cost OR
            NEW.inv_pmt_dscnt <> OLD.inv_pmt_dscnt OR
            NEW.ord_use_edi <> OLD.ord_use_edi OR
            NEW.cli_scac <> OLD.cli_scac OR
            NEW.ord_sb_authorized <> OLD.ord_sb_authorized OR
            NEW.ord_sb_assigned <> OLD.ord_sb_assigned OR
            NEW.ord_sb_truck_no <> OLD.ord_sb_truck_no OR
            NEW.ord_sb_load_order <> OLD.ord_sb_load_order OR
            NEW.ord_edi_invrpt <> OLD.ord_edi_invrpt OR
            NEW.ord_edi_invext <> OLD.ord_edi_invext OR
            NEW.ord_ptax1_amnt <> OLD.ord_ptax1_amnt OR
            NEW.ord_ptax2_amnt <> OLD.ord_ptax2_amnt OR
            NEW.ord_ptax3_amnt <> OLD.ord_ptax3_amnt OR
            NEW.ord_ptax4_amnt <> OLD.ord_ptax4_amnt OR
            NEW.ord_ptax5_amnt <> OLD.ord_ptax5_amnt OR
            NEW.ord_ttax1_amnt <> OLD.ord_ttax1_amnt OR
            NEW.ord_ttax2_amnt <> OLD.ord_ttax2_amnt OR
            NEW.ord_ttax3_amnt <> OLD.ord_ttax3_amnt OR
            NEW.ord_ttax4_amnt <> OLD.ord_ttax4_amnt OR
            NEW.ord_ttax5_amnt <> OLD.ord_ttax5_amnt OR
            NEW.ord_type <> OLD.ord_type OR
            NEW.mot_id <> OLD.mot_id OR
            NEW.ord_inv_method <> OLD.ord_inv_method OR
            NEW.ord_print_pckslp <> OLD.ord_print_pckslp OR
            NEW.ord_print_invoice <> OLD.ord_print_invoice OR
            -- NEW.ord_pmt_term_desc <> OLD.ord_pmt_term_desc OR
            NEW.ord_ship_term_desc <> OLD.ord_ship_term_desc OR
            NEW.en_creation <> OLD.en_creation OR
            NEW.ord_ord_sendmail <> OLD.ord_ord_sendmail OR
            NEW.ord_psc_sendmail <> OLD.ord_psc_sendmail OR
            NEW.ord_inv_sendmail <> OLD.ord_inv_sendmail OR
            NEW.ord_res_sendmail <> OLD.ord_res_sendmail OR
            NEW.ord_ord_email_fmt <> OLD.ord_ord_email_fmt OR
            NEW.ord_psc_email_fmt <> OLD.ord_psc_email_fmt OR
            NEW.ord_inv_email_fmt <> OLD.ord_inv_email_fmt OR
            NEW.ord_res_email_fmt <> OLD.ord_res_email_fmt OR
            NEW.ord_ord_email_subj <> OLD.ord_ord_email_subj OR
            NEW.ord_psc_email_subj <> OLD.ord_psc_email_subj OR
            NEW.ord_inv_email_subj <> OLD.ord_inv_email_subj OR
            NEW.ord_res_email_subj <> OLD.ord_res_email_subj OR
            NEW.ord_ord_email_to <> OLD.ord_ord_email_to OR
            NEW.ord_psc_email_to <> OLD.ord_psc_email_to OR
            NEW.ord_inv_email_to <> OLD.ord_inv_email_to OR
            NEW.ord_res_email_to <> OLD.ord_res_email_to OR
            NEW.ord_ord_email_cc <> OLD.ord_ord_email_cc OR
            NEW.ord_psc_email_cc <> OLD.ord_psc_email_cc OR
            NEW.ord_inv_email_cc <> OLD.ord_inv_email_cc OR
            NEW.ord_res_email_cc <> OLD.ord_res_email_cc OR
            NEW.ord_ord_email_bcc <> OLD.ord_ord_email_bcc OR
            NEW.ord_psc_email_bcc <> OLD.ord_psc_email_bcc OR
            NEW.ord_inv_email_bcc <> OLD.ord_inv_email_bcc OR
            NEW.ord_res_email_bcc <> OLD.ord_res_email_bcc OR
            NEW.ord_ord_email_sent <> OLD.ord_ord_email_sent OR
            NEW.ord_res_email_sent <> OLD.ord_res_email_sent OR
            NEW.ord_printed <> OLD.ord_printed OR
            NEW.ord_pkl_prnfmt <> OLD.ord_pkl_prnfmt OR
            NEW.ord_pkl_printed <> OLD.ord_pkl_printed OR
            NEW.ord_lbl_prnfmt <> OLD.ord_lbl_prnfmt OR
            NEW.ord_deposit <> OLD.ord_deposit OR
            NEW.ord_psl_sendmail <> OLD.ord_psl_sendmail OR
            NEW.ord_psl_email_fmt <> OLD.ord_psl_email_fmt OR
            NEW.ord_psl_email_subj <> OLD.ord_psl_email_subj OR
            NEW.ord_psl_email_to <> OLD.ord_psl_email_to OR
            NEW.ord_psl_email_cc <> OLD.ord_psl_email_cc OR
            NEW.ord_psl_email_bcc <> OLD.ord_psl_email_bcc OR
            NEW.ord_fpkg_cost <> OLD.ord_fpkg_cost OR
            NEW.ord_sb_pallet_nb <> OLD.ord_sb_pallet_nb OR
            NEW.ord_sb_load_note <> OLD.ord_sb_load_note OR
            NEW.ord_use_asn_edi <> OLD.ord_use_asn_edi OR
            NEW.ord_edi_asnrpt <> OLD.ord_edi_asnrpt OR
            NEW.ord_edi_asnext <> OLD.ord_edi_asnext OR
            NEW.ord_pnb_package <> OLD.ord_pnb_package OR
            NEW.ord_fpnb_package <> OLD.ord_fpnb_package OR
            NEW.ord_tnb_package <> OLD.ord_tnb_package OR
            NEW.ord_ftnb_package <> OLD.ord_ftnb_package OR
            NEW.ord_package_price <> OLD.ord_package_price OR
            NEW.ord_pqty <> OLD.ord_pqty OR
            NEW.ord_tqty <> OLD.ord_tqty OR
            NEW.ord_pweight <> OLD.ord_pweight OR
            NEW.ord_tweight <> OLD.ord_tweight OR
            NEW.ord_pvolume <> OLD.ord_pvolume OR
            NEW.ord_tvolume <> OLD.ord_tvolume OR
            NEW.cli_del_email <> OLD.cli_del_email OR
            NEW.inv_email <> OLD.inv_email OR
            NEW.ord_sync_to_web <> OLD.ord_sync_to_web OR
            NEW.ord_web_ord_no <> OLD.ord_web_ord_no OR
            NEW.ord_websync_id <> OLD.ord_websync_id
        THEN
            old_payload := (
                ''
                || sigm_str || ', '
                || 'OLD' || ', '
                || tg_table_name || ', '
                || tg_op || ', '
                || '[' || row_to_json(OLD) || ']'
            );
            new_payload := (
                ''
                || sigm_str || ', '
                || 'NEW' || ', '
                || tg_table_name || ', '
                || tg_op || ', '
                || '[' || row_to_json(NEW) || ']'
            );
        END IF;

    ELSE

        IF
            OLD <> NEW
        THEN
            old_payload := (
                ''
                || sigm_str || ', '
                || 'OLD' || ', '
                || tg_table_name || ', '
                || tg_op || ', '
                || '[' || row_to_json(OLD) || ']'
            );
             new_payload := (
                ''
                || sigm_str || ', '
                || 'NEW' || ', '
                || tg_table_name || ', '
                || tg_op || ', '
                || '[' || row_to_json(NEW) || ']'
            );
        END IF;
    END IF;

ELSIF
    tg_op = 'INSERT'
THEN
        new_payload := (
            ''
            || sigm_str || ', '
            || 'NEW' || ', '
            || tg_table_name || ', '
            || tg_op || ', '
            || '[' || row_to_json(NEW) || ']'
        );
END IF;

IF old_payload <> '' THEN

    IF (SELECT octet_length(old_payload::text) <= 8000) THEN
        PERFORM pg_notify(
            'logging', old_payload
        );
    ELSE
        PERFORM pg_notify(
            'logging',
            ''
            || sigm_str || ', '
            || tg_table_name || ', '
            || tg_op || ''
        );

    END IF;
END IF;

IF new_payload <> '' THEN

    IF (SELECT octet_length(new_payload::text) <= 8000) THEN
        PERFORM pg_notify(
            'logging', new_payload
        );
    ELSE
        PERFORM pg_notify(
            'logging',
            ''
            || sigm_str || ', '
            || tg_table_name || ', '
            || tg_op || ''
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
