from abc import ABC, abstractmethod
from os import path, getcwd, makedirs

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types, functions


class BaseOperator(ABC):

    # def __init__(self, data_dir='data'):
    #     self._data_dir = path.join(getcwd(), data_dir)
    
    # @property
    # def data_dir(self):
    #     if not path.exists(self._data_dir):
    #         makedirs(self._data_dir)
    #     return self._data_dir

    @abstractmethod
    def execute(self):
        pass


class BaseSpark(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.spark: SparkSession = SparkSession.builder.getOrCreate()
        # self._init_spark()
    
    # def _init_spark(self):  # Needed??? I don't think checkpoints make sense, instead save to parquet
    #     if not path.exists(self.spark_dir):
    #         makedirs(self.spark_dir)
    #     self.spark.sparkContext.setCheckpointDir(self.spark_dir)
    
    # @property
    # def spark_dir(self):
    #     return path.join(self.data_dir, 'spark')
    
    @staticmethod
    def concat_dataframes(dataframes: list[DataFrame]) -> DataFrame:
        out = dataframes.pop()
        for df in dataframes:
            out = out.union(df)
        return out

    @staticmethod
    def rename_columns(df: DataFrame, name_map:dict[str, str]) -> DataFrame:
        # for col in col_name_map.keys():
        #     dataframe = dataframe.withColumnRenamed(col, col_name_map[col])
        exprs = [f'{col} AS {name_map[col]}' if col in name_map else col for col in df.columns]
        return df.selectExpr(*exprs)

    @staticmethod
    def convert_df_datatypes(df:DataFrame, types:dict[str, list[str]]) -> DataFrame:
        if isinstance(next(iter(types.values())), str): # types expects {'type': ['col1', ...]}
            raise TypeError("types expects list of columns {'type': ['col1', ...]}")
        types = {types[k][i]: k for k in types.keys() for i in range(len(types[k]))}
        return df.withColumns({k: df[k].cast(types[k]) for k in types})

    @staticmethod
    def split_csv_column(df:DataFrame, csv_col:str) -> DataFrame:
        col_count = df.head().asDict()[csv_col].count(',') + 1
        split_col = functions.split(df[csv_col], ',')
        return df.withColumns({f'csv{i}': split_col.getItem(i) for i in range(col_count)})


