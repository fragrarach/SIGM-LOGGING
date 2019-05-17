import datetime
import quatro
from statements import write_snap_log, check_inc_table, write_inc_log
import data


# Checks inc tables for records added today, writes a snapshot of the record, starts timer for next snapshot
def scheduler_task(config):
    timestamp = str(datetime.datetime.now())
    for table_name in config.TABLES['snap']:
        ref_name = check_inc_table(config, table_name, timestamp)
        if ref_name:
            write_snap_log(config, table_name, ref_name, timestamp)


def listen_task(config, notify):
    raw_payload = notify.payload

    alert_table, alert_dict, timestamp, user, station, alert_age, alert_tg_op = data.payload_handler(raw_payload)
    str_columns, str_values = data.alert_handler(alert_dict)
    write_inc_log(config, alert_table, str_columns, timestamp, user, station, alert_age, alert_tg_op, str_values)
