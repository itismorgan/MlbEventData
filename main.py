import logging
import sys

from operators.base import Operator, BaseSparkOperator
from operators.get_retrosheet_data import RetrosheetToStorage
from operators.retrosheet_tables import EventsInitialParsing, EventTables


RETROSHEET_URL = "https://retrosheet.org/downloads/alldata.zip"  # 330MB at last count
EXPORT_DB_CONNECTION_TO_DESKTOP = True


def main():
    event_tables: list[Operator] = []
    init_logging()

    # Initialize operators
    get_rs_data = RetrosheetToStorage(url=RETROSHEET_URL)
    initial_rs_parse = EventsInitialParsing(source_data=get_rs_data.data_dir)
    for table_op in EventTables.TABLES:
        event_tables.append(table_op(source_data=initial_rs_parse.parq_path))

    # Run operators
    get_rs_data.execute()
    initial_rs_parse.execute()
    for table_op in event_tables:
        table_op.execute()
    initial_rs_parse.cleanup()
    
    if EXPORT_DB_CONNECTION_TO_DESKTOP:
        BaseSparkOperator.db.export_conn_to_desktop()


def init_logging(level: str = "info"):
    console_handler = logging.StreamHandler(sys.stdout)
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[console_handler],
    )


if __name__ == "__main__":
    main()
