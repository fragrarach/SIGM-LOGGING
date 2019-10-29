from os.path import dirname, abspath
from quatro import sigm_connect, log_connect


class Config:
    def __init__(self, main_file_path):
        self.main_file_path = main_file_path
        self.parent_dir = dirname(abspath(main_file_path))
        self.sigm_connection = None
        self.sigm_db_cursor = None
        self.log_connection = None
        self.log_db_cursor = None

    def sql_connections(self):
        self.sigm_connection, self.sigm_db_cursor = sigm_connect()
        self.log_connection, self.log_db_cursor = log_connect()

    TABLES = {
        'inc': [
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
        ],
        'snap': [
            'bill_of_materials_mat',
            'order_line',
            'part_kit',
            'contract_group_line',
            'contract_part_line'
        ]
    }

    COLUMNS = {
        'inc': [
            ['time_stamp', 'TIMESTAMP WITHOUT TIME ZONE'],
            ['user_name', 'TEXT'],
            ['station', 'TEXT'],
            ['age', 'TEXT'],
            ['tg_op', 'TEXT']
        ],
        'snap': [
            ['time_stamp', 'TIMESTAMP WITHOUT TIME ZONE']
        ]
    }

    TASK_SCHEDULE = [
        {
            'name': 'evening',
            'hour': 23,
            'minute': 0
        }
    ]

    LISTEN_CHANNEL = 'logging'
