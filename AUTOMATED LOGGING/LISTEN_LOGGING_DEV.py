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

conn_log = psycopg2.connect("host='192.168.0.57' dbname='LOG' user='SIGM' port='5493'")
conn_log.set_client_encoding("latin1")
conn_log.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

log_query = conn_log.cursor()

'''
CONSTANTS
'''

INC_TABLES = [
    'order_header',
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
        alert = payload.split(", ")[1]
        alert_age = alert.split(" ", 1)[0]
        alert_type = alert.split(" ", 1)[1]
        alert_load = payload.split("[")[1][:-1]
        alert_dict = json.loads(fr'{alert_load}')

        timestamp = datetime.datetime.now()
        log_message = f'{alert} by {user} on workstation {station} at {timestamp}\n'
        print(log_message)

        return alert_type, alert_dict, user, timestamp


# TODO: Write alert handler.
def alert_handler(alert_type, alert_dict, user, timestamp):
    if alert_type == 'PART NUMBER':
        pass
    elif alert_type == 'PART PRICE':
        pass
    elif alert_type == 'PACKING SLIP DATE':
        pass


# TODO: Write log handler.
def log_handler():
    pass


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

            alert_type, alert_dict, user, timestamp = payload_handler(raw_payload)
            alert_handler(alert_type, alert_dict, user, timestamp)


main()
