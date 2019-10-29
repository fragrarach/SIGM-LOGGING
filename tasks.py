from quatro import configuration as c
import datetime
from statements import write_snap_log, check_inc_table, write_inc_log
import data


# Checks inc tables for records added today, writes a snapshot of the record, starts timer for next snapshot
def scheduler_task():
    timestamp = str(datetime.datetime.now())
    for table_name in c.config.TABLES['snap']:
        ref_name = check_inc_table(table_name, timestamp)
        if ref_name:
            write_snap_log(table_name, ref_name, timestamp)


def listen_task(notify):
    raw_payload = notify.payload

    if data.payload_handler(raw_payload):
        alert_table, alert_dict, timestamp, user, station, alert_age, alert_tg_op = data.payload_handler(raw_payload)
        str_columns, str_values = data.alert_handler(alert_dict)
        write_inc_log(alert_table, str_columns, timestamp, user, station, alert_age, alert_tg_op, str_values)
