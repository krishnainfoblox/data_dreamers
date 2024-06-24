import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import pandas as pd

spark = SparkSession.builder.appName("FileValidation").getOrCreate()


file_path = "/Users/kkrishna/Library/CloudStorage/OneDrive-InfobloxInc/hackthon-2024/Project/data_dreamer_24-jun/data_dreamers/uploads/"

def handle_empty_values(df: DataFrame) -> DataFrame:
    for column in df.columns:
        df = df.withColumn(column, F.when(F.col(column) == "", None).otherwise(F.col(column)))
    return df


def comparedf(df1, df2, primary_key):
    df1 = handle_empty_values(df1)
    df2 = handle_empty_values(df2)

    src_distinct = df1.distinct()
    tgt_distinct = df2.distinct()

    result = {}
    result['record_count_check'] = 'PASS' if src_distinct.count() == tgt_distinct.count() else 'FAIL'

    null_check_columns = [col for col in df1.columns if col != primary_key]
    null_check_df1 = df1.where(
        df1[primary_key].isNull() | F.array_contains(F.array([F.col(col).isNull() for col in null_check_columns]),
                                                     True))
    null_check_df2 = df2.where(
        df2[primary_key].isNull() | F.array_contains(F.array([F.col(col).isNull() for col in null_check_columns]),
                                                     True))

    result['null_check'] = 'PASS' if null_check_df1.count() == 0 and null_check_df2.count() == 0 else 'FAIL'

    duplicate_check_df1 = df1.groupBy(primary_key).count().filter("count > 1").count()
    duplicate_check_df2 = df2.groupBy(primary_key).count().filter("count > 1").count()

    result['duplicate_check'] = 'PASS' if duplicate_check_df1 == 0 and duplicate_check_df2 == 0 else 'FAIL'

    minus_result = df1.select(df1.columns).subtract(df2.select(df2.columns))
    result['source_minus_target'] = 'PASS' if minus_result.isEmpty() else 'FAIL'

    result_df = pd.DataFrame([result])

    return result_df


def read_csv(file_path: str) -> DataFrame:
    df = spark.read.option("header", "true").csv(file_path)
    return handle_empty_values(df)


def read_json(file_path: str) -> DataFrame:
    df = spark.read.format("json").option("multiline", "true").load(file_path)
    return handle_empty_values(df)


def read_parquet(file_path: str) -> DataFrame:
    df = spark.read.format("parquet").load(file_path)
    return handle_empty_values(df)


def read_file(file_path: str, file_type: str) -> DataFrame:
    if file_type == "CSV":
        return read_csv(file_path)
    elif file_type == "JSON":
        return read_json(file_path)
    elif file_type == "PARQUET":
        return read_parquet(file_path)
    else:
        raise ValueError("Unsupported file type")


if __name__ == "__main__":
    file1_path = sys.argv[1]
    file1_type = sys.argv[2]
    file2_path = sys.argv[3]
    file2_type = sys.argv[4]
    primary_key = sys.argv[5]

    df1 = read_file(file1_path, file1_type)
    df2 = read_file(file2_path, file2_type)

    result_df = comparedf(df1, df2, primary_key)

    result_file_path = "/Users/kkrishna/Library/CloudStorage/OneDrive-InfobloxInc/hackthon-2024/Project/code-hackathon-2024/result/file_validation_result.xlsx"
    result_df.to_excel(result_file_path, index=False)

    print(f"Validation completed. Results saved to {result_file_path}")
