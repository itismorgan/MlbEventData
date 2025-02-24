from shutil import rmtree

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, monotonically_increasing_id, first
from pyspark.sql.types import StructType, StructField, StringType

from operators.base import Operator, BaseSparkOperator


class EventsInitialParsing(BaseSparkOperator):
    data_name = "events_initial"
    event_dirs = ["events", "ngl_b", "ngl_e", "allstar"]
    schema = StructType(
        [
            StructField("event_type", StringType(), False),
            StructField("col2", StringType()),
            StructField("col3", StringType()),
            StructField("col4", StringType()),
            StructField("col5", StringType()),
            StructField("col6", StringType()),
            StructField("col7", StringType()),
        ]
    )

    _DELETE_DATA = True  # delete intermediate parquet when cleanup method is called

    @property
    def source_paths(self):
        return [self.source_data / d for d in self.event_dirs]

    @property
    def parq_path(self):
        return str(self.data_dir / self.data_name)

    def cleanup(self):
        if self._DELETE_DATA:
            rmtree(self.parq_path)

    def execute(self):
        events = [
            self.spark.read.csv(str(p), schema=self.schema) for p in self.source_paths
        ]
        events = self.concat_dataframes(events)

        gi = _GameIdUDF()
        set_game_id = udf(gi.set_row_id)
        events = events.withColumns(
            {
                "game_id": set_game_id(events["event_type"], events["col2"]),
                "record_id": monotonically_increasing_id(),
            }
        )

        events.write.mode("overwrite").parquet(self.parq_path)


class EventTables(BaseSparkOperator):
    data_name: str

    TABLES: list[Operator] = []

    def get_event_data(self) -> DataFrame:
        return self.spark.read.parquet(str(self.source_data))

    def write_table(self, df: DataFrame) -> None:
        parq_path = self.data_dir / self.data_name
        self.db.write_table(df=df, table_name=self.data_name, parquet_path=parq_path)

    @classmethod
    def register_table(cls, subcls: "EventTables"):
        cls.TABLES.append(subcls)
        return subcls


@EventTables.register_table
class GameEventsTable(EventTables):
    data_name = "game_events"

    def execute(self):
        col_names = {
            "col2": "inning",
            "col3": "is_home_team",
            "col4": "player_id",
            "col5": "count",
            "col6": "pitches",
            "col7": "play_description",
        }
        type_map = {"short": ["inning"], "boolean": ["is_home_team"]}
        df = self.get_event_data()
        df = df.filter(df["event_type"] == "play").select(
            ["record_id", "game_id", "col2", "col3", "col4", "col5", "col6", "col7"]
        )
        df = self.rename_columns(df, col_names)
        df = self.convert_df_datatypes(df, type_map)
        self.write_table(df)


@EventTables.register_table
class GameInfoTable(EventTables):
    data_name = "game_info"

    def execute(self):
        type_map = {
            "short": ["number", "temp", "timeofgame", "windspeed"],
            "integer": ["attendance"],
            "date": ["date"],
            "timestamp": ["inputtime"],
        }
        df = self.get_event_data()
        df = (
            df.filter(df["event_type"] == "info")
            .select("game_id", "col2", "col3")
            .groupBy("game_id")
            .pivot("col2")
            .agg(first("col3"))
        )
        df = self.convert_df_datatypes(df, types=type_map)
        self.write_table(df)


@EventTables.register_table
class GameDataTable(EventTables):
    data_name = "game_data"

    def execute(self):
        col_names = {
            "col2": "metric_type",
            "col3": "player_code",
            "col4": "metric_value",
        }
        type_map = {"short": ["metric_value"]}
        df = self.get_event_data()
        df = df.filter(df["event_type"] == "data").select(
            ["game_id", "col2", "col3", "col4"]
        )
        df = self.rename_columns(df, col_names)
        df = self.convert_df_datatypes(df, type_map)
        self.write_table(df)


@EventTables.register_table
class GameRostersTable(EventTables):
    data_name = "game_rosters"

    def execute(self):
        col_names = {
            "col2": "player_id",
            "col3": "player_name",
            "col4": "is_home_team",
            "col5": "batting_order",
            "col6": "fielding_position",
        }
        type_map = {
            "boolean": ["is_home_team"],
            "short": ["batting_order", "fielding_position"],
        }
        df = self.get_event_data()
        df = df.filter(df["event_type"].isin(["start", "sub"])).select(
            [
                "game_id",
                "record_id",
                "event_type",
                "col2",
                "col3",
                "col4",
                "col5",
                "col6",
            ]
        )
        df = self.rename_columns(df, col_names)
        df = self.convert_df_datatypes(df, type_map)
        self.write_table(df)


@EventTables.register_table
class UmpireChangeEventsTable(EventTables):
    data_name = "umpire_change_events"

    def execute(self):
        col_names = {"csv1": "inning", "csv2": "position", "csv3": "umpire_id"}
        type_map = {"short": ["inning"]}
        df = self.get_event_data()
        df = df.filter(df["col2"].contains("umpchange"))
        df = self.split_csv_column(df, "col2")
        df = df.select(["record_id", "game_id", "csv1", "csv2", "csv3"])
        df = self.rename_columns(df, col_names)
        df = self.convert_df_datatypes(df, type_map)
        self.write_table(df)


@EventTables.register_table
class CommentsTable(EventTables):
    data_name = "comments"

    def execute(self):
        col_names = {"col2": "comment"}
        df = self.get_event_data()
        df = (
            df.filter(df["event_type"] == "com")
            .filter(~df["col2"].contains("umpchange"))
            .select(["record_id", "game_id", "col2"])
        )
        df = self.rename_columns(df, col_names)
        self.write_table(df)


@EventTables.register_table
class AdjustmentsTable(EventTables):
    data_name = "adjustments"

    def execute(self):
        df = self.get_event_data()
        df = (
            df.filter(
                df["event_type"].isin(["badj", "padj", "ladj", "radj", "presadj"])
            )
            .withColumn(
                "adjustment_events_map",
                _adjustment_values_map(df["event_type"], df["col2"], df["col3"]),
            )
            .select(["record_id", "game_id", "event_type", "adjustment_events_map"])
        )
        self.write_table(df)


###  UDFs  ###


class _GameIdUDF:
    def __init__(self):
        self.current_id = None

    def set_row_id(self, event_type, col2):
        if event_type == "id":
            self.current_id = col2
        if not self.current_id:
            raise ValueError("current_id cannot be None.")
        return self.current_id


@udf
def _adjustment_values_map(event_type, col2, col3):
    if event_type == "badj":
        out = {"event_name": "Batter Adjustment", "player_id": col2, "hand": col3}
    elif event_type == "padj":
        out = {"event_name": "Pitcher Adjustment", "player_id": col2, "hand": col3}
    elif event_type == "ladj":
        out = {
            "event_name": "Lineup Adjustment",
            "batting_team": col2,
            "batting_order_position": col3,
        }
    elif event_type == "radj":
        out = {"event_name": "Runner Adjustment", "player_id": col2, "base": col3}
    elif event_type == "presadj":
        out = {
            "event_name": "Pitcher Responsiblity Adjustment",
            "player_id": col2,
            "base": col3,
        }
    else:
        out = None
    return out
