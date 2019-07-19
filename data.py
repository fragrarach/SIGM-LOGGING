import re
import json
import datetime
from quatro import log


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
        log(log_message)

        return alert_table, alert_dict, timestamp, user, station, alert_age, alert_tg_op
    return


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


# Generate string of column names and values to be insert into log table
def alert_handler(alert_dict):
    columns = []
    for key in alert_dict:
        columns.append(key)
    str_columns = ', '.join(columns)

    row = alert_dict.values()
    str_values = row_value_str(row)

    return str_columns, str_values
