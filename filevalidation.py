from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import pandas as pd
from datetime import datetime

spark = SparkSession.builder.appName("ES").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def handle_empty_values(df: DataFrame) -> DataFrame:
    for column in df.columns:
        df = df.withColumn(column, F.when(F.col(column) == "", None).otherwise(F.col(column)))
    return df


def comparedf(df1, df2, primary_key):
    #spark = SparkSession.builder.appName("DataFrameComparison").getOrCreate()

    # Handle empty values in DataFrames
    df1 = handle_empty_values(df1)
    df2 = handle_empty_values(df2)

    # Perform comparison
    src_distinct = df1.distinct()
    tgt_distinct = df2.distinct()

    result = {}

    # Record count check
    result['record_count_check'] = 'PASS' if src_distinct.count() == tgt_distinct.count() else 'FAIL'

    # NULL Check
    null_check_columns = [col for col in df1.columns if col != primary_key]
    null_check_df1 = df1.where(
        df1[primary_key].isNull() | F.array_contains(F.array([F.col(col).isNull() for col in null_check_columns]),
                                                     True))
    null_check_df2 = df2.where(
        df2[primary_key].isNull() | F.array_contains(F.array([F.col(col).isNull() for col in null_check_columns]),
                                                     True))

    result['null_check'] = 'PASS' if null_check_df1.count() == 0 and null_check_df2.count() == 0 else 'FAIL'

    # Duplicate Check
    duplicate_check_df1 = df1.groupBy(primary_key).count().filter("count > 1").count()
    duplicate_check_df2 = df2.groupBy(primary_key).count().filter("count > 1").count()

    result['duplicate_check'] = 'PASS' if duplicate_check_df1 == 0 and duplicate_check_df2 == 0 else 'FAIL'

    # Source MINUS Target
    minus_result = df1.select(df1.columns).subtract(df2.select(df2.columns))
    result['source_minus_target'] = 'PASS' if minus_result.isEmpty() else 'FAIL'

    # Creating a DataFrame for result
    result_df = pd.DataFrame([result])

    return result_df


def read_csv(file_path: str) -> DataFrame:
    df = spark.read.option("header", "true").csv(file_path)
    return handle_empty_values(df)


def read_json(file_path: str) -> DataFrame:
    df = spark.read.format("json").option("multiline", "true").load(file_path)
    return handle_empty_values(df)


def read_parquet(file_path: str) -> DataFrame:
    df = spark.read.format("parquet").option("header", "true").load(file_path)
    return handle_empty_values(df)


def read_excel(file_path: str) -> DataFrame:
    df = pd.read_excel(file_path)
    return handle_empty_values(df)
