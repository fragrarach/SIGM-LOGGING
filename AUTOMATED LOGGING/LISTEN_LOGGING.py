import smtplib
import psycopg2.extensions
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

INC_TABLES = ['part',
              'part_price',
              'part_supplier',
              'invoicing',
              'bill_of_materials_mat',
              'part_kit',
              'contract',
              'contract_group_line',
              'contract_part_line']

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


def drop_triggers():
    tables = table_names()
    for column in tables:
        for table in column:
            sql_exp = f'DROP TRIGGER IF EXISTS logging_notify on {table} CASCADE'
            sigm_query.execute(sql_exp)
            print(f'{table} log trigger dropped.')


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
        alert_load = payload.split("{")[1][:-1]
        timestamp = datetime.datetime.now()

        log_message = f'{alert} by {user} on workstation {station} at {timestamp}\n'
        print(log_message)
        print(alert_load)
        alert_handler(alert, user, timestamp, alert_load)
        # log_handler(timestamp, alert, ref_type, ref, user, station)

        # return alert, ref, user


def alert_handler(alert, user, timestamp, alert_load):
    log_load = {}
    if alert == 'RENAMED PART':
        old_load = alert_load.split(', ')[0]
        old_prt_no = old_load.split(':')[1]
        new_load = alert_load.split(', ')[1]
        new_prt_no = new_load.split(':')[1]
        log_load['old_prt_no'] = old_prt_no
        log_load['new_prt_no'] = new_prt_no
        print(f'Part number {log_load.get("old_prt_no")} '
              f'changed to {log_load.get("new_prt_no")} by {user} at {timestamp}')
    elif alert == 'CHANGED SALE PRICE':
        pass
    elif alert == 'CHANGED PURCHASE PRICE':
        pass
    elif alert == 'CHANGED CONTRACT':
        pass


# TODO: Write log handler.
def log_handler():
    pass


def main():
    while 1:
        conn_sigm.poll()
        conn_sigm.commit()
        while conn_sigm.notifies:

            notify = conn_sigm.notifies.pop()
            raw_payload = notify.payload
            print(raw_payload)
            # alert, ref, user = payload_handler(raw_payload)
            payload_handler(raw_payload)


main()
