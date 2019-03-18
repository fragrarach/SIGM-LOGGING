import os
import select
import datetime
import sys
import re
import ast
import psycopg2
import psycopg2.extras
import psycopg2.extensions
from os.path import dirname, abspath

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

conn_sigm = psycopg2.connect("host='192.168.0.57' dbname='DEV' user='SIGM' port='5493'")
conn_sigm.set_client_encoding("latin1")
conn_sigm.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

sigm_listen = conn_sigm.cursor()
sigm_listen.execute("LISTEN logging;")
sigm_query = conn_sigm.cursor()

conn_log = psycopg2.connect("host='192.168.0.57' dbname='LOG' user='SIGM' port='5493'")
conn_log.set_client_encoding("latin1")
conn_log.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

log_query = conn_log.cursor()

# tables = ['order_header',
#           'order_line',
#           'purchase_order_header',
#           'purchase_order_line',
#           'bill_of_materials_mat',
#           'part',
#           'client']


def table_names():
    sql_exp = f'SELECT table_name ' \
              f'FROM information_schema.tables ' \
              f'WHERE table_schema = \'public\' ' \
              f'AND table_type = \'BASE TABLE\''
    sigm_query.execute(sql_exp)
    result_set = sigm_query.fetchall()

    tables = []
    for column in result_set:
        for cell in column:
            tables.append(cell)
    return tables


def log_tables():
    tables = table_names()
    for table in tables:
        sql_exp = f'SELECT * FROM table_columns(\'{table}\')'
        sigm_query.execute(sql_exp)
        result_set = sigm_query.fetchall()

        columns = []
        for column in result_set:
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
                  f'tg_op           TEXT,' \
                  f'primary_key     TEXT,' \
                  f'pkey_ref             TEXT,' \
                  f'{str_columns})'
        log_query.execute(sql_exp)
        print(f'{table} log table checked.')


def log_notify_triggers():
    tables = table_names()
    for table in tables:
        sql_exp = f'DROP TRIGGER IF EXISTS logging_notify ON {table}; ' \
                  f'CREATE TRIGGER logging_notify ' \
                  f'    AFTER UPDATE OR INSERT OR DELETE' \
                  f'    ON {table} ' \
                  f'    FOR EACH ROW ' \
                  f'    EXECUTE PROCEDURE logging_notify(); '
        sigm_query.execute(sql_exp)
        print(f'{table} log trigger checked.')


def log_notify_function():
    sql_exp = f'CREATE OR REPLACE FUNCTION logging_notify() ' \
              f'    RETURNS trigger AS ' \
              f'$BODY$ ' \
              f'DECLARE ' \
              f'sigm_str    TEXT; ' \
              f'primary_key NAME; ' \
              f'pkey_ref    RECORD; ' \
              f'BEGIN ' \
              f'sigm_str := ( ' \
              f'    SELECT application_name ' \
              f'    FROM pg_stat_activity ' \
              f'    WHERE pid IN ( ' \
              f'        SELECT pg_backend_pid() ' \
              f'    ) ' \
              f'); ' \
              f'primary_key := ( ' \
              f'    SELECT * ' \
              f'    FROM table_primary_keys(\'\' || tg_table_name || \'\') ' \
              f'); ' \
              f'EXECUTE format( ' \
              f'    \'SELECT %s FROM %s WHERE %s = $1.%s\', primary_key, tg_table_name, primary_key, primary_key ' \
              f') ' \
              f'USING NEW ' \
              f'INTO   pkey_ref; ' \
              f'PERFORM pg_notify( ' \
              f'        \'logging\', \'\' ' \
              f'        || tg_table_name || \', \' ' \
              f'        || tg_op || \', \' ' \
              f'        || primary_key || \', \' ' \
              f'        || pkey_ref || \', \' ' \
              f'        || sigm_str || \'\' ' \
              f'); ' \
              f'RETURN NULL; ' \
              f'END; ' \
              f'$BODY$ ' \
              f'    LANGUAGE plpgsql VOLATILE ' \
              f'    COST 100; ' \
              f'ALTER FUNCTION logging_notify() ' \
              f'    OWNER TO "SIGM";'
    sigm_query.execute(sql_exp)


def short_handler(table, timestamp, user, tg_op, station, pkey, pkey_ref):
    sql_exp = f'SELECT attname FROM table_columns(\'{table}\')'
    sigm_query.execute(sql_exp)
    result_set = sigm_query.fetchall()

    columns = []
    for column in result_set:
        for cell in column:
            columns.append(cell)
    columns = ', '.join(columns)

    sql_exp = f'SELECT * FROM {table} where {pkey} = {pkey_ref}'
    sigm_query.execute(sql_exp)
    result_set = sigm_query.fetchall()

    values = []
    for column in result_set:
        for cell in column:
            if type(cell) != str:
                if cell is None:
                    cell = 'Null'
                if type(cell) == datetime.date:
                    cell = f'\'{cell}\'::DATE'
                values.append(str(cell))
            else:
                cell = cell.replace('\'', '\'\'')
                cell = f'\'{cell}\''
                values.append(cell)
    values = ', '.join(values[:])
    print(values)

    sql_exp = f'INSERT INTO {table} (time_stamp, user_name, tg_op, station, primary_key, pkey_ref, {columns}) ' \
              f'VALUES (\'{timestamp}\', \'{user}\', \'{tg_op}\', \'{station}\', \'{pkey}\', \'{pkey_ref}\', {values})'
    log_query.execute(sql_exp)
    print(sql_exp)


def long_handler(table, timestamp, user, tg_op, station, pkey, pkey_ref, load):
    pattern = re.compile(
        r'(\"[a-z0-9]*?[_a-z0-9]*?[_a-z0-9]*?\"):(\"*?[a-zA-Z0-9\\.]*?\"*?(?:(?!,\").)*)')
    matches = re.findall(pattern, load)
    values = []
    columns = []
    for match in matches:
        column = match[0][1:-1]
        columns.append(column)
        value = match[1]
        if value == '""':
            value = 'Null'
        if value[0] == '"' or value[-1] == '"':
            while value[0] == '"' or value[-1] == '"':
                if value[0] == '"':
                    value = value[1:]
                if value[-1] == '"':
                    value = value[:-1]
            while value[0] == '\\' or value[-1] == '\\':
                if value[0] == '\\':
                    value = value[1:]
                if value[-1] == '\\':
                    value = value[:-1]
            value = value.replace('\'', '\'\'')
            value = f'E\'{value}\''
        if match[1][0] != '"' and match[1][-1] != '"':
            value = match[1]
        values.append(value)
    columns = ', '.join(columns)
    values = ', '.join(values)

    sql_exp = f'INSERT INTO {table} (time_stamp, user_name, tg_op, station, primary_key, pkey_ref, {columns}) ' \
              f'VALUES (\'{timestamp}\', \'{user}\', \'{tg_op}\', \'{station}\', \'{pkey}\', \'{pkey_ref}\', {values})'
    print(sql_exp)
    log_query.execute(sql_exp)


def payload_handler(payload):
    load_type = payload.split(", ")[0]
    table = payload.split(", ")[1]
    tg_op = payload.split(", ")[2]
    sigm_string = payload.split(", ")[3]

    app = re.findall(r'(?<=SIGM[0-9]{3} a)(.*)(?=\.EXE u)', sigm_string)[0]
    if app == 'SIGMWIN':
        user = re.findall(r'(?<=aSIGMWIN\.EXE u)(.*)(?= m)', sigm_string)[0]
        station = re.findall(r'(?<= w)(.*)$', sigm_string)[0]
    else:
        user = 'SIGM'
        station = 'fileserver'

    timestamp = datetime.datetime.now()

    if load_type == 'long_load' or load_type == 'short_load':
        pkey = payload.split(", ")[4]
        pkey_ref = payload.split(", ")[5][1:-1]
        if load_type == 'short_load':
            short_handler(table, timestamp, user, tg_op, station, pkey, pkey_ref)
        if load_type == 'long_load':
            load = payload.split("{")[1][:-1]
            long_handler(table, timestamp, user, tg_op, station, pkey, pkey_ref, load)


# delete_tables = table_names()
# for table in delete_tables:
#     sql_exp = f'DROP TABLE if exists {table}'
#     log_query.execute(sql_exp)
#     print(f'{table} log table dropped.')
#
# for table in delete_tables:
#     sql_exp = f'DROP TRIGGER IF EXISTS logging_notify on {table} CASCADE'
#     sigm_query.execute(sql_exp)
#     print(f'{table} log trigger dropped.')

log_tables()
# log_notify_function()
log_notify_triggers()

while 1:
    conn_sigm.poll()
    conn_sigm.commit()
    while conn_sigm.notifies:
        notify = conn_sigm.notifies.pop()
        raw_payload = notify.payload
        print(raw_payload)
        payload_handler(raw_payload)



