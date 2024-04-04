import glob

from pyspark.sql import SparkSession, DataFrame

from my_access import POSTGRES_CONNECTION_PROPERTIES, POSTGRES_URL


def get_spark(app_name="spark app"):
    return SparkSession \
        .builder \
        .appName(app_name) \
        .getOrCreate()


def read_csv_to_df(path, year: str = "*"):
    spark = SparkSession.builder.getOrCreate()

    path_wc = f"{path}/{year}/*.csv"
    print(f"loading csv files into spark dataframe: {path_wc}")
    csv_files = glob.glob(path_wc)
    print(csv_files)
    df = spark.read.csv(csv_files, header=True, inferSchema=True)

    return df


def read_table(schema: str, table_name: str):
    spark = SparkSession.builder.getOrCreate()
    return (
        spark
        .read.jdbc(
            url=POSTGRES_URL,
            table=f"{schema}.{table_name}",
            properties=POSTGRES_CONNECTION_PROPERTIES)
    )

def write_table(df: DataFrame, database: str, table_name: str):
    table = f"{database}.{table_name}"
    df.write.jdbc(url=POSTGRES_URL,
                  table=table,
                  mode="overwrite",
                  properties=POSTGRES_CONNECTION_PROPERTIES)
    print(f"Table was successfully writen as: {table}")

def write_partition():
    pass

