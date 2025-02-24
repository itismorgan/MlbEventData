import duckdb
import sqlite3
from typing import Protocol
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions


class Operator(Protocol):
    data_dir: Path | str

    def execute(self) -> None: 
        ...


class Db(Protocol):
    @classmethod
    def write_table(cls, df: DataFrame, **kwargs) -> None:
        ...

    @classmethod
    def export_conn_to_desktop(cls) -> None: 
        ...


class DuckdbDb:
    DB_NAME = "mlb_events.duckdb"
    _conn = None

    @classmethod
    def get_conn(cls):
        if not cls._conn:
            cls._conn = duckdb.connect(cls.DB_NAME)
        return cls._conn

    @classmethod
    def write_table(
        cls, df: DataFrame, table_name: str, parquet_path: Path | str
    ) -> None:
        df.write.mode("overwrite").parquet(str(parquet_path))
        s = f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{parquet_path}/*.parquet')"
        cls.get_conn().execute(s)

    @classmethod
    def export_conn_to_desktop(cls):
        _export_conn(conn_name="duckdb", db_name=cls.DB_NAME)


class SqliteDb:
    # MVP implementation, Sqlite implementation requires
    # converting DFs to memory which limits performance

    DB_NAME = "mlb_events.db"
    _init_con = False

    @classmethod
    def write_table(cls, df: DataFrame, table_name: str):
        df = df.toPandas()
        with sqlite3.connect(cls.DB_NAME) as conn:
            df.to_sql(
                table_name, conn, if_exists="replace", index=False, chunksize=1000
            )

    @classmethod
    def export_conn_to_desktop(cls):
        _export_conn(conn_name="sqlite", db_name=cls.DB_NAME)


class BaseSparkOperator:
    CORES = "all"
    MEMORY = "8g"
    _LOG_LEVEL = "ERROR"

    db: Db = DuckdbDb
    _spark = None

    def __init__(self, source_data: Path | str, data_dir: Path | str = "data"):
        self.source_data = Path(source_data)
        self.data_dir = Path.cwd() / data_dir / "spark"

    @property
    def spark(self):
        return self._get_spark()

    @classmethod
    def _get_spark(cls):
        # TODO: full spark warning surpression
        cores = "*" if cls.CORES == "all" else cls.CORES
        memory = cls.MEMORY
        if not cls._spark:
            cls._spark: SparkSession = (
                SparkSession.builder.appName("MlbSpark")
                .master(f"local[{cores}]")
                .config("spark.driver.memory", memory)
                .config("spark.executor.memory", memory)
                .getOrCreate()
            )
            cls._spark.sparkContext.setLogLevel(cls._LOG_LEVEL)
        return cls._spark

    @staticmethod
    def concat_dataframes(dataframes: list[DataFrame]) -> DataFrame:
        out = dataframes.pop()
        for df in dataframes:
            out = out.union(df)
        return out

    @staticmethod
    def rename_columns(df: DataFrame, name_map: dict[str, str]) -> DataFrame:
        exprs = [
            f"{col} AS {name_map[col]}" if col in name_map else col
            for col in df.columns
        ]
        return df.selectExpr(*exprs)

    @staticmethod
    def convert_df_datatypes(df: DataFrame, types: dict[str, list[str]]) -> DataFrame:
        if isinstance(
            next(iter(types.values())), str
        ):  # types expects {'type': ['col1', ...]}
            raise TypeError("types expects list of columns {'type': ['col1', ...]}")
        types = {types[k][i]: k for k in types.keys() for i in range(len(types[k]))}
        return df.withColumns({k: df[k].cast(types[k]) for k in types})

    @staticmethod
    def split_csv_column(df: DataFrame, csv_col: str) -> DataFrame:
        col_count = df.head().asDict()[csv_col].count(",") + 1
        split_col = functions.split(df[csv_col], ",")
        return df.withColumns(
            {f"csv{i}": split_col.getItem(i) for i in range(col_count)}
        )


# Helpers
def _export_conn(conn_name: str, db_name):
    conn = Path.cwd() / db_name
    dest = _desktop_path_if_exists_else_home()
    with open(dest / f"mlb_{conn_name}_conn.txt", "w") as file:
        file.write(str(conn))


def _desktop_path_if_exists_else_home():
    out = Path.home()
    desktop = out / "Desktop"
    if desktop.exists():
        out = desktop
    return out
