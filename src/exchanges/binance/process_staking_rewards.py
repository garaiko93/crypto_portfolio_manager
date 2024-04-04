from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, sum, collect_list, lit
from pyspark.sql.types import StringType, StructField, StructType, DoubleType

from config import BINANCE_EXCHANGE_NAME, REFINED_DB, REFINED_TRADES, REFINED_STAKING_REWARDS
from utils.spark_utils import write_table


def process_transactions(operations, coins, changes):
    sent_coin = None
    sent_amount = None
    fee_coin = None
    fee_amount = None
    received_coin = None
    received_amount = None
    for op, coin, change in zip(operations, coins, changes):
        change = abs(change)
        if op == "Transaction Spend":
            sent_coin = coin
            sent_amount = change
        elif op == "Transaction Fee":
            fee_coin = coin
            fee_amount = change
        elif op == "Transaction Buy":
            received_coin = coin
            received_amount = change
        elif op == "Transaction Sold":
            sent_coin = coin
            sent_amount = change
        elif op == "Transaction Revenue":
            received_coin = coin
            received_amount = change

    return sent_coin, sent_amount, fee_coin, fee_amount, received_coin, received_amount

schema = StructType([
    StructField("sent_coin", StringType(), True),
    StructField("sent_amount", DoubleType(), True),
    StructField("fee_coin", StringType(), True),
    StructField("fee_amount", DoubleType(), True),
    StructField("received_coin", StringType(), True),
    StructField("received_amount", DoubleType(), True)
])


def refine_staking_rewards(df: DataFrame) -> DataFrame:
    summary_df = df.groupBy("User_Id", "UTC_Time", "Account", "Coin", "year_month", "date_key").agg(sum("Change").alias("Change"))

    # Expand struct columns into separate columns
    final_df = summary_df.select(
        col("UTC_Time").alias("timestamp"),
        col("User_Id").alias("user_id"),
        col("Account").alias("account"),
        col("year_month"),
        col("date_key"),
        col("Coin").alias("received_coin"),
        col("Change").alias("received_amount"),
        lit(BINANCE_EXCHANGE_NAME).alias("exchange"))

    print(f"table has {final_df.count()} records.")

    write_table(final_df, REFINED_DB, REFINED_STAKING_REWARDS)

    return final_df

