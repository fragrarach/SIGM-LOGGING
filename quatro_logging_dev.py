from sigm import *
import listen
from sql import add_triggers, add_tables, extend_tables, init_snap_tables
from tasks import start_timer
from config import Config


def main():
    add_sql_files()

    # drop_tables(Config.INC_TABLES, Config.LOG_DB_CURSOR)
    # drop_tables(Config.SNAP_TABLES, Config.LOG_DB_CURSOR)
    # drop_triggers()

    add_triggers()
    add_tables(Config.INC_TABLES)
    add_tables(Config.SNAP_TABLES)
    extend_tables(Config.INC_TABLES, Config.INC_COLUMNS, Config.LOG_DB_CURSOR)
    extend_tables(Config.SNAP_TABLES, Config.SNAP_COLUMNS, Config.LOG_DB_CURSOR)

    init_snap_tables()
    start_timer()

    listen.listen()


if __name__ == "__main__":
    main()

