import datetime
from threading import Timer
from sigm import sql_query, tabular_data
from config import Config
from sql import write_snap_log, snap_log_ref


# Set snapshot timer delay, optionally delay by 24 hours (for every snapshot subsequent to the first)
def set_snap_timer():
    now = datetime.datetime.today()
    then = now.replace(day=now.day, hour=23, minute=0, second=0, microsecond=0)

    delta = then - now
    secs = delta.seconds
    hours = round((secs / 60) / 60, 2)
    print(f'Snapshot timer scheduled for {hours} hours from now.')
    return secs


# Start snapshot timer, optionally delay by 24 hours (for every snapshot subsequent to the first)
def start_snap_timer():
    secs = set_snap_timer()
    snap_timer = Timer(secs, snapshot_handler)
    snap_timer.start()


# Checks inc tables for records added today, writes a snapshot of the record, starts timer for next snapshot
def snapshot_handler():
    timestamp = str(datetime.datetime.now())
    for table_name in Config.SNAP_TABLES:
        ref_name = check_inc_table(table_name, timestamp)
        if ref_name:
            write_snap_log(table_name, ref_name, timestamp)
    start_snap_timer()


# Checks inc table for records added today, gets ref_name if new increments exist
def check_inc_table(table_name, timestamp):
    sql_exp = f'SELECT * FROM {table_name} WHERE time_stamp::DATE = \'{timestamp}\'::DATE'
    result_set = sql_query(sql_exp, Config.LOG_DB_CURSOR)
    table = tabular_data(result_set)
    if table:
        ref_name = snap_log_ref(table_name)
        return ref_name
