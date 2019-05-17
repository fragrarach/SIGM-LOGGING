from quatro import sigm_connect, log_connect


class Config:
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

    def __init__(self):
        self.sigm_connection, self.sigm_db_cursor = sigm_connect(Config.LISTEN_CHANNEL)
        self.log_connection, self.log_db_cursor = log_connect()
