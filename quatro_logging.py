from quatro import init_app_log_dir, log, add_sql_files, start_scheduler, listen
import config
from statements import add_triggers, add_tables, extend_tables, init_snap_tables
from tasks import listen_task, scheduler_task


def main():
    init_app_log_dir()
    log(f'Starting {__file__}')
    logging_config = config.Config()
    add_sql_files(logging_config)

    # drop_tables(logging_config)
    # drop_triggers(logging_config)

    add_triggers(logging_config)
    add_tables(logging_config)
    extend_tables(logging_config)

    init_snap_tables(logging_config)
    start_scheduler(logging_config, scheduler_task)

    listen(logging_config, listen_task)


if __name__ == "__main__":
    main()
