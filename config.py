import psycopg2.extensions
from sigm import sigm_connect, log_connect


# PostgreSQL DB connection configs
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)


class Config:
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

    LISTEN_CHANNEL = 'logging'

    SIGM_CONNECTION, SIGM_DB_CURSOR = sigm_connect(LISTEN_CHANNEL)
    LOG_CONNECTION, LOG_DB_CURSOR = log_connect()
