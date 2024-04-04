import os
import zipfile

from pyspark.sql import Window
from pyspark.sql.functions import row_number, col, lit, date_format

from config import ROOT_DIR, BINANCE_CSV_PATH, RAW_DB, BINANCE_RAW_TABLE
from utils.spark_utils import read_csv_to_df, get_spark, write_table

RAW_DATA_PATH = f"{ROOT_DIR}/data/raw"
EXCHANGES_RAW_FILES = f"{RAW_DATA_PATH}/exchanges"
def extract_files(path, exchange):
    for file in os.listdir(path):
        if file.__contains__(".zip"):
            file_name = file.split(".")[0]
            print(f"its a zip file, extracting {file}")
            extract_path = f"{EXCHANGES_RAW_FILES}/{exchange}/csv/{file_name}"
            with zipfile.ZipFile(f"{path}/{file}", 'r') as zip_ref:
                zip_ref.extractall(extract_path)
            print(f"extracted file at: {extract_path}")

def preprocess_raw_files(exchange):
    originals_path = f"{EXCHANGES_RAW_FILES}/{exchange}/originals"
    if os.path.exists(originals_path):
        extract_files(originals_path, exchange)
    else:
        print(f"path {originals_path} does not exist")


if __name__ == "__main__":
    spark = get_spark()
    print("starting ingestion")

    # this extracts .zip files into different folders as csv
    preprocess_raw_files("binance")

    # load files per year/month or all at once with spark
    df = read_csv_to_df(BINANCE_CSV_PATH).distinct()

    # non contextual transformations before writing into db or parquet
    df_trans = (df
                .withColumn("year_month", date_format(col("UTC_Time"), "yyyyMM"))
                .withColumn("date_key", date_format(col("UTC_Time"), "yyyyMMdd")))

    w = Window().orderBy("UTC_Time").partitionBy("year_month")

    df_trans = df_trans.withColumn("row_number", row_number().over(w))

    df_trans.show()
    print(f"table has {df_trans.count()} records.")

    write_table(df_trans, RAW_DB, BINANCE_RAW_TABLE)



