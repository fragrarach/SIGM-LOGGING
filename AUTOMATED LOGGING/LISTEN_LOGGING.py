import smtplib
import psycopg2.extensions
import re
import datetime
import os
from os.path import dirname, abspath
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


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


# def log_handler(timestamp, alert, ref_type, ref, user, station):
#     sql_exp = f'INSERT INTO alerts (time_stamp, alert, ref_type, reference, user_name, station) ' \
#               f'VALUES (\'{timestamp}\', \'{alert}\', \'{ref_type}\', \'{ref}\', \'{user}\', \'{station}\')'
#     log_query.execute(sql_exp)


def payload_handler(payload):
    ref_type = payload.split(", ")[0]
    ref = payload.split(", ")[1]
    alert = payload.split(", ")[2]
    sigm_string = payload.split(", ")[3]
    user = re.findall(r'(?<=aSIGMWIN\.EXE u)(.*)(?= m)', sigm_string)[0]
    station = re.findall(r'(?<= w)(.*)$', sigm_string)[0]
    timestamp = datetime.datetime.now()

    log_message = f'{alert} on {ref_type} {ref} by {user} on workstation {station} at {timestamp}\n'
    print(log_message)
    log_handler(timestamp, alert, ref_type, ref, user, station)

    return alert, ref, user


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


while 1:
    conn_sigm.poll()
    conn_sigm.commit()
    while conn_sigm.notifies:

        notify = conn_sigm.notifies.pop()
        raw_payload = notify.payload
        print(raw_payload)
        # alert, ref, user = payload_handler(raw_payload)
