import datetime

from config import Config
from sigm import sql_query, tabular_data
import listen


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


# Add incremental log triggers to all tables in INC_TABLES list
def add_triggers():
    for table in Config.INC_TABLES:
        sql_exp = f'DROP TRIGGER IF EXISTS logging_notify ON {table}; ' \
                  f'CREATE TRIGGER logging_notify ' \
                  f'    AFTER UPDATE OR INSERT OR DELETE ' \
                  f'    ON {table} ' \
                  f'    FOR EACH ROW ' \
                  f'    EXECUTE PROCEDURE logging_notify()'
        Config.SIGM_DB_CURSOR.execute(sql_exp)
        print(f'{table} log trigger added.')


# Drop incremental log triggers from all tables in INC_TABLES list
def drop_triggers():
    tables = table_names(Config.SIGM_DB_CURSOR)
    for column in tables:
        for table in column:
            sql_exp = f'DROP TRIGGER IF EXISTS logging_notify on {table} CASCADE'
            Config.SIGM_DB_CURSOR.execute(sql_exp)
            print(f'{table} log trigger dropped.')


# Create incremental log tables on LOG DB for every table in INC_TABLES list
def add_tables(table_list):
    for table_name in table_list:
        str_columns = column_name_str(table_name, Config.SIGM_DB_CURSOR, True)

        if table_list == Config.SNAP_TABLES:
            table_name += '_snap'

        sql_exp = f'CREATE TABLE IF NOT EXISTS {table_name}(' \
                  f'{str_columns})'
        Config.LOG_DB_CURSOR.execute(sql_exp)
        print(f'{table_name} log table checked.')


# Initialize snapshot logging tables.
def init_snap_tables():
    timestamp = datetime.datetime.now()
    for table_name in Config.SNAP_TABLES:
        dest_table_name = table_name + '_snap'
        if not whole_table(dest_table_name, Config.LOG_DB_CURSOR):
            copy_table(table_name, dest_table_name, Config.SIGM_DB_CURSOR, Config.LOG_DB_CURSOR)
            sql_exp = f'UPDATE {dest_table_name} ' \
                      f'SET time_stamp = \'{timestamp}\''
            Config.LOG_DB_CURSOR.execute(sql_exp)


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
    str_columns = column_name_str(table_name, Config.SIGM_DB_CURSOR)

    sql_exp = f'SELECT DISTINCT {ref_name} FROM {table_name} WHERE time_stamp::DATE = \'{timestamp}\'::DATE'
    result_set = sql_query(sql_exp, Config.LOG_DB_CURSOR)
    ref_table = tabular_data(result_set)
    for ref_row in ref_table:
        ref = ref_row[0]
        sql_exp = f'SELECT * FROM {table_name} WHERE {ref_name} = {ref}'
        result_set = sql_query(sql_exp, Config.SIGM_DB_CURSOR)
        table_data = tabular_data(result_set)
        rows = len(table_data)
        print(f'Writing {rows} rows to {table_name}_snap for {ref_name} {ref}')
        for row in table_data:
            str_values = listen.row_value_str(row)
            sql_exp = \
                f'INSERT INTO {table_name}_snap ({str_columns}, time_stamp) ' \
                f'VALUES ({str_values}, \'{timestamp}\')'
            Config.LOG_DB_CURSOR.execute(sql_exp)


# Add columns to a table.
def extend_tables(table_list, column_list, cursor):
    for table in table_list:
        if table_list == Config.SNAP_TABLES:
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
        if table_list == Config.SNAP_TABLES:
            table = table + '_snap'

        sql_exp = f'DROP TABLE IF EXISTS {table} CASCADE'
        cursor.execute(sql_exp)
        print(f'{table} log table dropped.')


# Copy table from one DB to another
def copy_table(source_table_name, dest_table_name, source_cursor, dest_cursor=None):
    if dest_cursor is None:
        dest_cursor = source_cursor

    str_columns = column_name_str(source_table_name, Config.SIGM_DB_CURSOR)
    source_table = whole_table(source_table_name, Config.SIGM_DB_CURSOR)
    rows = len(source_table)

    print(
        f'Copying {rows} rows table {source_table_name} using {source_cursor} to {dest_table_name} using {dest_cursor}')

    for row in source_table:
        str_values = listen.row_value_str(row)
        sql_exp = fr'INSERT INTO {dest_table_name} ({str_columns}) VALUES ({str_values})'
        dest_cursor.execute(sql_exp)
    print(f'Copying table {source_table_name} to {dest_table_name} complete.')


# Checks inc table for records added today, gets ref_name if new increments exist
def check_inc_table(table_name, timestamp):
    sql_exp = f'SELECT * FROM {table_name} WHERE time_stamp::DATE = \'{timestamp}\'::DATE'
    result_set = sql_query(sql_exp, Config.LOG_DB_CURSOR)
    table = tabular_data(result_set)
    if table:
        ref_name = snap_log_ref(table_name)
        return ref_name


# Insert named variables returned by payload handler and payload generated by alert_handler into log table
def write_inc_log(alert_table, str_columns, timestamp, user, station, alert_age, alert_tg_op, str_values):
    sql_exp = fr"INSERT INTO {alert_table} (tg_op, time_stamp, user_name, station, age, {str_columns}) " \
              fr"VALUES ('{alert_tg_op}', '{timestamp}', '{user}', '{station}', '{alert_age}', {str_values})"
    Config.LOG_DB_CURSOR.execute(sql_exp)


if __name__ == "__main__":
    init_snap_tables()
    drop_triggers()
    add_triggers()
