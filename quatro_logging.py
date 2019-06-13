import quatro
import config
from statements import add_triggers, add_tables, extend_tables, init_snap_tables
from tasks import listen_task, scheduler_task


def main():
    logging_config = config.Config()
    quatro.add_sql_files(logging_config)

    # drop_tables(logging_config)
    # drop_triggers(logging_config)

    add_triggers(logging_config)
    add_tables(logging_config)
    extend_tables(logging_config)

    init_snap_tables(logging_config)
    quatro.start_scheduler(logging_config, scheduler_task)

    quatro.listen(logging_config, listen_task)


if __name__ == "__main__":
    main()
