from quatro import init_app_log_dir, log, add_sql_files, start_scheduler, listen, configuration as c, sql_query
from config import Config
from statements import add_triggers, add_tables, extend_tables, init_snap_tables
from tasks import listen_task, scheduler_task


def main():
    c.config = Config(__file__)
    init_app_log_dir()
    log(f'Starting {__file__}')
    c.config.sql_connections()
    add_sql_files()

    # drop_tables(logging_config)
    # drop_triggers(logging_config)

    add_triggers()
    add_tables()
    extend_tables()

    init_snap_tables()
    start_scheduler(scheduler_task)

    listen(listen_task)


if __name__ == "__main__":
    main()
