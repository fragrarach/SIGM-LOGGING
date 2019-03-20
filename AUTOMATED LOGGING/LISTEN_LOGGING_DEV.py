import smtplib
import psycopg2.extensions
import json
import re
import datetime
import os
from os.path import dirname, abspath
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

'''
PSYCOPG SETTINGS
'''


def dev_check():
    raw_filename = os.path.basename(__file__)
    removed_extension = raw_filename.split('.')[0]
    last_word = removed_extension.split('_')[-1]
    if last_word == 'DEV':
        return True
    else:
        return False


psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

if dev_check():
    conn_sigm = psycopg2.connect("host='192.168.0.57' dbname='DEV' user='SIGM' port='5493'")
else:
    conn_sigm = psycopg2.connect("host='192.168.0.250' dbname='QuatroAir' user='SIGM' port='5493'")
conn_sigm.set_client_encoding("latin1")
conn_sigm.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

sigm_listen = conn_sigm.cursor()
sigm_listen.execute("LISTEN logging;")
sigm_query = conn_sigm.cursor()

if dev_check():
    conn_log = psycopg2.connect("host='192.168.0.57' dbname='LOG' user='SIGM' port='5493'")
else:
    conn_log = psycopg2.connect("host='192.168.0.250' dbname='LOG' user='SIGM' port='5493'")
conn_log.set_client_encoding("latin1")
conn_log.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

log_query = conn_log.cursor()

'''
CONSTANTS
'''

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

SNAP_TABLES = ['bill_of_materials_mat',
               'part_kit',
               'contract',
               'contract_group_line',
               'contract_part_line']

'''
FUNCTIONS
'''


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


def scalar_data(result_set):
    for row in result_set:
        for cell in row:
            if type(cell) == str:
                cell = cell.strip()
            return cell


def production_query(sql_exp):
    sigm_query.execute(sql_exp)
    result_set = sigm_query.fetchall()
    return result_set


def table_names():
    sql_exp = f'SELECT * FROM table_names()'
    result_set = production_query(sql_exp)
    tables = tabular_data(result_set)
    return tables


def table_columns(table):
    sql_exp = f'SELECT * FROM table_columns(\'{table}\')'
    result_set = production_query(sql_exp)
    columns = tabular_data(result_set)
    return columns


def add_inc_triggers():
    for table in INC_TABLES:
        sql_exp = f'SELECT add_inc_log_tg_funcs(\'{table}\')'
        sigm_query.execute(sql_exp)
        print(f'{table} log trigger added.')


def drop_inc_triggers():
    tables = table_names()
    for column in tables:
        for table in column:
            sql_exp = f'DROP TRIGGER IF EXISTS logging_notify on {table} CASCADE'
            sigm_query.execute(sql_exp)
            print(f'{table} log trigger dropped.')


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
        log_query.execute(sql_exp)
        print(f'{table} log table checked.')


def drop_inc_tables():
    for table in INC_TABLES:
        sql_exp = f'DROP TABLE IF EXISTS {table} CASCADE'
        log_query.execute(sql_exp)
        print(f'{table} log table dropped.')


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
        alert_load = payload.split("[")[1][:-1]
        alert_dict = json.loads(fr'{alert_load}')

        timestamp = datetime.datetime.now()
        log_message = f'{alert_age} {alert_table} by {user} on workstation {station} at {timestamp}\n'
        print(log_message)

        return alert_table, alert_dict, timestamp, user, station, alert_age


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
                value = "'" + value + "'"
        values.append(value)
    str_values = ', '.join(values)

    return str_columns, str_values


def log_handler(alert_table, str_columns, timestamp, user, station, alert_age, str_values):
    sql_exp = f"INSERT INTO {alert_table} (time_stamp, user_name, station, age, {str_columns}) " \
              f"VALUES ('{timestamp}', '{user}', '{station}', '{alert_age}', {str_values})"
    log_query.execute(sql_exp)


def main():
    # drop_inc_tables()
    add_inc_triggers()
    add_inc_tables()
    while 1:
        conn_sigm.poll()
        conn_sigm.commit()
        while conn_sigm.notifies:
            notify = conn_sigm.notifies.pop()
            raw_payload = notify.payload

            alert_table, alert_dict, timestamp, user, station, alert_age = payload_handler(raw_payload)
            str_columns, str_values = alert_handler(alert_dict)
            log_handler(alert_table, str_columns, timestamp, user, station, alert_age, str_values)


main()
