import json
import re
import datetime
from threading import Timer
from sigm import *

# PostgreSQL DB connection configs
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

# Incremental log tables
INC_TABLES = [
    'order_header',
    'order_line',
    'part',
    'part_price',
    # 'part_supplier',
    'invoicing',
    'bill_of_materials_mat',
    'part_kit',
    'contract',
    'contract_group_line',
    'contract_part_line'
]

# TODO : Implement snapshot logging
# Snapshot log tables
SNAP_TABLES = [
    'bill_of_materials_mat',
    'order_line',
    'part_kit',
    'contract_group_line',
    'contract_part_line'
]

INC_COLUMNS = [
    ['time_stamp', 'TIMESTAMP WITHOUT TIME ZONE'],
    ['user_name', 'TEXT'],
    ['station', 'TEXT'],
    ['age', 'TEXT'],
    ['tg_op', 'TEXT']
]

SNAP_COLUMNS = [
    ['time_stamp', 'TIMESTAMP WITHOUT TIME ZONE']
]


# Call table_names() PL/PG function, pull all table names from public schema
def table_names(cursor):
    sql_exp = f'SELECT * FROM table_names()'
    result_set = sql_query(sql_exp, cursor)
    tables = tabular_data(result_set)
    return tables


# Call table_columns() PL/PG function, pull all column names/attribute names/attribute numbers of a table
def table_columns(table, cursor):
    sql_exp = f'SELECT * FROM table_columns(\'{table}\')'
    result_set = sql_query(sql_exp, cursor)
    columns = tabular_data(result_set)
    return columns


# Select an entire table
def whole_table(table_name, cursor):
    sql_exp = f'SELECT * FROM {table_name}'
    result_set = sql_query(sql_exp, cursor)
    table = tabular_data(result_set)
    return table


# Pass table name, return comma separated string of column names, optionally include attributes
def column_name_str(table_name, cursor, include_attributes=False):
    raw_columns = table_columns(table_name, cursor)
    columns = []
    str_columns = ''
    for column in raw_columns:
        if include_attributes:
            pair = []
            for cell in column:
                pair.append(cell)
            str_pair = ' '.join(pair)
            columns.append(str_pair)
        else:
            column_name = column[0]
            columns.append(column_name)
        str_columns = ', '.join(columns)
    return str_columns


# Pass row, return comma separated string of values
def row_value_str(row):
    values = []
    for value in row:
        if value is None:
            value = 'Null'
        if type(value) in (datetime.date, datetime.time):
            value = str(value)
        if type(value) != str:
            value = str(value)
        else:
            if value != 'Null':
                if "'" in value:
                    value = value.replace("'", "''")
                if "\\" in value:
                    value = value.replace("\\", "\\\\")
                value = "'" + value + "'"
        values.append(value)
    str_values = ', '.join(values)
    return str_values


# Add incremental log triggers to all tables in INC_TABLES list
def add_triggers():
    for table in INC_TABLES:
        sql_exp = f'DROP TRIGGER IF EXISTS logging_notify ON {table}; ' \
                  f'CREATE TRIGGER logging_notify ' \
                  f'    AFTER UPDATE OR INSERT OR DELETE ' \
                  f'    ON {table} ' \
                  f'    FOR EACH ROW ' \
                  f'    EXECUTE PROCEDURE logging_notify()'
        sigm_db_cursor.execute(sql_exp)
        print(f'{table} log trigger added.')


# Drop incremental log triggers from all tables in INC_TABLES list
def drop_triggers():
    tables = table_names(sigm_db_cursor)
    for column in tables:
        for table in column:
            sql_exp = f'DROP TRIGGER IF EXISTS logging_notify on {table} CASCADE'
            sigm_db_cursor.execute(sql_exp)
            print(f'{table} log trigger dropped.')


# Create incremental log tables on LOG DB for every table in INC_TABLES list
def add_tables(table_list):
    for table_name in table_list:
        str_columns = column_name_str(table_name, sigm_db_cursor, True)

        if table_list == SNAP_TABLES:
            table_name += '_snap'

        sql_exp = f'CREATE TABLE IF NOT EXISTS {table_name}(' \
                  f'{str_columns})'
        log_db_cursor.execute(sql_exp)
        print(f'{table_name} log table checked.')


# Initialize snapshot logging tables.
def init_snap_tables():
    timestamp = datetime.datetime.now()
    for table_name in SNAP_TABLES:
        dest_table_name = table_name + '_snap'
        if not whole_table(dest_table_name, log_db_cursor):
            copy_table(table_name, dest_table_name, sigm_db_cursor, log_db_cursor)
            sql_exp = f'UPDATE {dest_table_name} ' \
                      f'SET time_stamp = \'{timestamp}\''
            log_db_cursor.execute(sql_exp)


# Set snapshot timer delay, optionally delay by 24 hours (for every snapshot subsequent to the first)
def set_snap_timer(delay=False):
    now = datetime.datetime.today()
    then = now.replace(day=now.day, hour=23, minute=0, second=0, microsecond=0)

    delta = then - now
    secs = delta.seconds if delay is False else delta.seconds + 86400
    hours = round((secs / 60) / 60, 2)
    print(f'Snapshot timer scheduled for {hours} hours from now.')
    return secs


# Start snapshot timer, optionally delay by 24 hours (for every snapshot subsequent to the first)
def start_snap_timer(delay=False):
    secs = set_snap_timer(delay)
    snap_timer = Timer(secs, snapshot_handler)
    snap_timer.start()


# Checks inc tables for records added today, writes a snapshot of the record, starts timer for next snapshot
def snapshot_handler():
    timestamp = str(datetime.datetime.now())
    for table_name in SNAP_TABLES:
        ref_name = check_inc_table(table_name, timestamp)
        if ref_name:
            write_snap_log(table_name, ref_name, timestamp)
    start_snap_timer(True)


# Checks inc table for records added today, gets ref_name if new increments exist
def check_inc_table(table_name, timestamp):
    sql_exp = f'SELECT * FROM {table_name} WHERE time_stamp::DATE = \'{timestamp}\'::DATE'
    result_set = sql_query(sql_exp, log_db_cursor)
    table = tabular_data(result_set)
    if table:
        ref_name = snap_log_ref(table_name)
        return ref_name


# Return appropriate column to use as reference when writing to snap log
def snap_log_ref(table_name):
    ref_name = ''
    if table_name == 'bill_of_materials_mat':
        ref_name = 'prt_master_id'
    elif table_name == 'order_line':
        ref_name = 'ord_id'
    elif table_name == 'part_kit':
        ref_name = 'pkt_master_prt_id'
    elif table_name == 'contract_group_line':
        ref_name = 'con_id'
    elif table_name == 'contract_part_line':
        ref_name = 'con_id'
    return ref_name


def write_snap_log(table_name, ref_name, timestamp):
    str_columns = column_name_str(table_name, sigm_db_cursor)

    sql_exp = f'SELECT DISTINCT {ref_name} FROM {table_name} WHERE time_stamp::DATE = \'{timestamp}\'::DATE'
    result_set = sql_query(sql_exp, log_db_cursor)
    ref_table = tabular_data(result_set)
    for ref_row in ref_table:
        ref = ref_row[0]
        sql_exp = f'SELECT * FROM {table_name} WHERE {ref_name} = {ref}'
        result_set = sql_query(sql_exp, sigm_db_cursor)
        table_data = tabular_data(result_set)
        rows = len(table_data)
        print(f'Writing {rows} rows to {table_name}_snap for {ref_name} {ref}')
        for row in table_data:
            str_values = row_value_str(row)
            sql_exp = \
                f'INSERT INTO {table_name}_snap ({str_columns}, time_stamp) ' \
                f'VALUES ({str_values}, \'{timestamp}\')'
            log_db_cursor.execute(sql_exp)


# Add columns to a table.
def extend_tables(table_list, column_list, cursor):
    for table in table_list:
        if table_list == SNAP_TABLES:
            table += '_snap'
        for column_pair in column_list:
            column = column_pair[0]
            attribute = column_pair[1]
            try:
                sql_exp = f'ALTER TABLE IF EXISTS {table} ' \
                          f'ADD COLUMN {column} {attribute};'
                cursor.execute(sql_exp)
                print(f'Added {column} column to {table} table.')
            except:
                print(f'{column} column already exists on {table} table.')


# Drop tables in a list on a specific DB
def drop_tables(table_list, cursor):
    for table in table_list:
        if table_list == SNAP_TABLES:
            table = table + '_snap'

        sql_exp = f'DROP TABLE IF EXISTS {table} CASCADE'
        cursor.execute(sql_exp)
        print(f'{table} log table dropped.')


# Copy table from one DB to another
def copy_table(source_table_name, dest_table_name, source_cursor, dest_cursor=None):
    if dest_cursor is None:
        dest_cursor = source_cursor

    str_columns = column_name_str(source_table_name, sigm_db_cursor)
    source_table = whole_table(source_table_name, sigm_db_cursor)
    rows = len(source_table)

    print(
        f'Copying {rows} rows table {source_table_name} using {source_cursor} to {dest_table_name} using {dest_cursor}')

    for row in source_table:
        str_values = row_value_str(row)
        sql_exp = fr'INSERT INTO {dest_table_name} ({str_columns}) VALUES ({str_values})'
        dest_cursor.execute(sql_exp)
    print(f'Copying table {source_table_name} to {dest_table_name} complete.')


# Split payload string, return named variables
def payload_handler(payload):
    sigm_string = payload.split(", ")[0]

    app = re.findall(r'(?<=SIGM[0-9]{3} a)(.*)(?=\.EXE u)', sigm_string)[0]
    if app == 'SIGMWIN':
        user = re.findall(r'(?<=aSIGMWIN\.EXE u)(.*)(?= m)', sigm_string)[0]
        station = re.findall(r'(?<= w)(.*)$', sigm_string)[0]
    else:
        user = 'SIGM'
        station = 'fileserver'

    if user != 'SIGM':
        alert_age = payload.split(", ")[1]
        alert_table = payload.split(", ")[2]
        alert_tg_op = payload.split(", ")[3]
        alert_load = payload.split("[")[1][:-1]
        alert_dict = json.loads(fr'{alert_load}')

        timestamp = datetime.datetime.now()
        log_message = f'{alert_tg_op} {alert_age} {alert_table} by {user} on workstation {station} at {timestamp}\n'
        print(log_message)

        return alert_table, alert_dict, timestamp, user, station, alert_age, alert_tg_op


# Generate string of column names and values to be insert into log table
def alert_handler(alert_dict):
    columns = []
    for key in alert_dict:
        columns.append(key)
    str_columns = ', '.join(columns)

    row = alert_dict.values()
    str_values = row_value_str(row)

    return str_columns, str_values


# Insert named variables returned by payload handler and payload generated by alert_handler into log table
def write_inc_log(alert_table, str_columns, timestamp, user, station, alert_age, alert_tg_op, str_values):
    sql_exp = fr"INSERT INTO {alert_table} (tg_op, time_stamp, user_name, station, age, {str_columns}) " \
              fr"VALUES ('{alert_tg_op}', '{timestamp}', '{user}', '{station}', '{alert_age}', {str_values})"
    log_db_cursor.execute(sql_exp)


def main():
    channel = 'logging'
    global sigm_connection, sigm_db_cursor, log_connection, log_db_cursor
    sigm_connection, sigm_db_cursor = sigm_connect(channel)
    log_connection, log_db_cursor = log_connect()

    add_sql_files()

    # drop_tables(INC_TABLES, log_db_cursor)
    # drop_tables(SNAP_TABLES, log_db_cursor)
    # drop_triggers()

    add_triggers()
    add_tables(INC_TABLES)
    add_tables(SNAP_TABLES)
    extend_tables(INC_TABLES, INC_COLUMNS, log_db_cursor)
    extend_tables(SNAP_TABLES, SNAP_COLUMNS, log_db_cursor)

    init_snap_tables()
    start_snap_timer(False)

    while 1:
        try:
            sigm_connection.poll()
        except:
            print('Database cannot be accessed, PostgreSQL service probably rebooting')
            try:
                sigm_connection.close()
                sigm_connection, sigm_db_cursor = sigm_connect(channel)
                log_connection.close()
                log_connection, log_db_cursor = log_connect()
            except:
                pass
        else:
            sigm_connection.commit()
            while sigm_connection.notifies:
                notify = sigm_connection.notifies.pop()
                raw_payload = notify.payload

                alert_table, alert_dict, timestamp, user, station, alert_age, alert_tg_op = payload_handler(raw_payload)
                str_columns, str_values = alert_handler(alert_dict)
                write_inc_log(alert_table, str_columns, timestamp, user, station, alert_age, alert_tg_op, str_values)


main()
