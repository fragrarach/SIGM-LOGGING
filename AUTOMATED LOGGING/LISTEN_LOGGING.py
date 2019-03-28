import json
import re
import datetime
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
    'invoicing'
    # 'bill_of_materials_mat',
    # 'part_kit',
    # 'contract',
    # 'contract_group_line',
    # 'contract_part_line'
    ]

# TODO : Implement snapshot logging
# Snapshot log tables
SNAP_TABLES = ['bill_of_materials_mat',
               'part_kit',
               'contract',
               'contract_group_line',
               'contract_part_line']


# Convert tabular query result to list (2D array)
def tabular_data(result_set):
    lines = []
    for row in result_set:
        line = []
        for cell in row:
            if type(cell) == str:
                cell = cell.strip()
            line.append(cell)
        lines.append(line)
    return lines


# Convert scalar query result to singleton variable of any data type
def scalar_data(result_set):
    for row in result_set:
        for cell in row:
            if type(cell) == str:
                cell = cell.strip()
            return cell


# Query production database
def sigm_db_query(sql_exp):
    sigm_db_cursor.execute(sql_exp)
    result_set = sigm_db_cursor.fetchall()
    return result_set


# Call table_names() PL/PG function, pull all table names from public schema
def table_names():
    sql_exp = f'SELECT * FROM table_names()'
    result_set = sigm_db_query(sql_exp)
    tables = tabular_data(result_set)
    return tables


# Call table_columns() PL/PG function, pull all column names/attribute names/attribute numbers of a table
def table_columns(table):
    sql_exp = f'SELECT * FROM table_columns(\'{table}\')'
    result_set = sigm_db_query(sql_exp)
    columns = tabular_data(result_set)
    return columns


# Add incremental log triggers to all tables in INC_TABLES list
def add_inc_triggers():
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
def drop_inc_triggers():
    tables = table_names()
    for column in tables:
        for table in column:
            sql_exp = f'DROP TRIGGER IF EXISTS logging_notify on {table} CASCADE'
            sigm_db_cursor.execute(sql_exp)
            print(f'{table} log trigger dropped.')


# Create incremental log tables on LOG DB for every table in INC_TABLES list
def add_inc_tables():
    for table in INC_TABLES:
        raw_columns = table_columns(table)
        columns = []
        for column in raw_columns:
            pair = []
            for cell in column:
                pair.append(cell)
            str_pair = ' '.join(pair)
            columns.append(str_pair)
        str_columns = ', '.join(columns)

        sql_exp = f'CREATE TABLE IF NOT EXISTS {table}(' \
                  f'time_stamp      TIMESTAMP WITHOUT TIME ZONE,' \
                  f'user_name       TEXT,' \
                  f'station         TEXT,' \
                  f'age             TEXT,' \
                  f'{str_columns})'
        log_db_cursor.execute(sql_exp)
        print(f'{table} log table checked.')

        try:
            sql_exp = f'ALTER TABLE IF EXISTS {table} ' \
                      f'ADD COLUMN tg_op TEXT;'
            log_db_cursor.execute(sql_exp)
            print(f'{table} new column(s) added.')
        except:
            print(f'{table} new column(s) already exist.')


# Drop incremental log tables on LOG DB for every table in INC_TABLES list
def drop_inc_tables():
    for table in INC_TABLES:
        sql_exp = f'DROP TABLE IF EXISTS {table} CASCADE'
        log_db_cursor.execute(sql_exp)
        print(f'{table} log table dropped.')


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

    values = []
    for value in alert_dict.values():
        if value is None:
            value = 'Null'
        if type(value) == datetime.date:
            value = f'\'{value}\'::DATE'
        if type(value) != str:
            value = str(value)
        else:
            if value != 'Null':
                if "'" in value:
                    value = value.replace("'", "''")
                    print(value)
                value = "'" + value + "'"
        values.append(value)
    str_values = ', '.join(values)

    return str_columns, str_values


# Insert named variables returned by payload handler and payload generated by alert_handler into log table
def log_handler(alert_table, str_columns, timestamp, user, station, alert_age, alert_tg_op, str_values):
    sql_exp = fr"INSERT INTO {alert_table} (tg_op, time_stamp, user_name, station, age, {str_columns}) " \
              fr"VALUES ('{alert_tg_op}', '{timestamp}', '{user}', '{station}', '{alert_age}', {str_values})"
    log_db_cursor.execute(sql_exp)


def main():
    channel = 'logging'
    global sigm_connection, sigm_db_cursor, log_connection, log_db_cursor
    sigm_connection, sigm_db_cursor = sigm_connect(channel)
    log_connection, log_db_cursor = log_connect()

    add_sql_files()
    # drop_inc_tables()
    # drop_inc_triggers()
    add_inc_triggers()
    add_inc_tables()

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
                log_handler(alert_table, str_columns, timestamp, user, station, alert_age, alert_tg_op, str_values)


main()
