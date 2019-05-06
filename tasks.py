import datetime
from threading import Timer
from config import Config
from sql import write_snap_log, check_inc_table


# Start snapshot timer, optionally delay by 24 hours (for every snapshot subsequent to the first)
def start_timer():
    secs = set_timer()
    snap_timer = Timer(secs, task)
    snap_timer.start()


# Set snapshot timer delay, optionally delay by 24 hours (for every snapshot subsequent to the first)
def set_timer():
    now = datetime.datetime.today()
    then = now.replace(day=now.day, hour=23, minute=0, second=0, microsecond=0)

    delta = then - now
    secs = delta.seconds
    hours = round((secs / 60) / 60, 2)
    print(f'Snapshot timer scheduled for {hours} hours from now.')
    return secs


# Checks inc tables for records added today, writes a snapshot of the record, starts timer for next snapshot
def task():
    timestamp = str(datetime.datetime.now())
    for table_name in Config.SNAP_TABLES:
        ref_name = check_inc_table(table_name, timestamp)
        if ref_name:
            write_snap_log(table_name, ref_name, timestamp)
        start_timer()


if __name__ == "__main__":
    start_timer()
    set_timer()
    task()
