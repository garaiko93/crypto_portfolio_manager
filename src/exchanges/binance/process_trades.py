from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, sum, collect_list, lit
from pyspark.sql.types import StringType, StructField, StructType, DoubleType

from config import BINANCE_EXCHANGE_NAME, REFINED_DB, REFINED_TRADES, STABLE_COINS_AND_FIAT
from utils.spark_utils import write_table_in_postgres

#
# def process_transactions(operations, coins, changes):
#     sent_coin = None
#     sent_amount = None
#     fee_coin = None
#     fee_amount = None
#     received_coin = None
#     received_amount = None
#     for op, coin, change in zip(operations, coins, changes):
#         change = abs(change)
#         if op == "Transaction Spend":
#             sent_coin = coin
#             sent_amount = change
#         elif op == "Transaction Fee":
#             fee_coin = coin
#             fee_amount = change
#         elif op == "Transaction Buy":
#             received_coin = coin
#             received_amount = change
#         elif op == "Transaction Sold":
#             sent_coin = coin
#             sent_amount = change
#         elif op == "Transaction Revenue":
#             received_coin = coin
#             received_amount = change
#
#     if sent_coin in STABLE_COINS_AND_FIAT:
#         action = "buy"
#     elif received_coin in STABLE_COINS_AND_FIAT:
#         action = "sell"
#     else:
#         action = "exchange"
#
#     if action == "buy":
#         price = sent_amount / received_amount
#     else:
#         price = received_amount / sent_amount
#
#     return sent_coin, sent_amount, fee_coin, fee_amount, received_coin, received_amount, price, action
#
# schema = StructType([
#     StructField("sent_coin", StringType(), True),
#     StructField("sent_amount", DoubleType(), True),
#     StructField("fee_coin", StringType(), True),
#     StructField("fee_amount", DoubleType(), True),
#     StructField("received_coin", StringType(), True),
#     StructField("received_amount", DoubleType(), True),
#     StructField("price", DoubleType(), True),
#     StructField("action", StringType(), True)
# ])
#
#
# def refine_binance_trades(df: DataFrame) -> DataFrame:
#     # this avoids a trade that has been splitted between different operations at the same time
#     summary_df = df.groupBy("User_Id", "UTC_Time", "Account", "Operation", "Coin", "year_month", "date_key").agg(sum("Change").alias("Change"))
#
#     grouped_df = summary_df.groupBy("User_Id", "UTC_Time", "Account", "year_month", "date_key").agg(
#         collect_list("Operation").alias("Operations"),
#         collect_list("Coin").alias("Coins"),
#         collect_list("Change").alias("Changes")
#     )
#
#     # Register UDF
#     process_transactions_udf = udf(process_transactions, schema)
#
#     # Apply UDF to DataFrame
#     processed_df = (grouped_df
#                     .withColumn("processed_transactions",
#                                 process_transactions_udf(col("Operations"), col("Coins"), col("Changes"))))
#
#     # Expand struct columns into separate columns
#     final_df = processed_df.select(
#         col("UTC_Time").alias("timestamp"),
#         col("User_Id").alias("user_id"),
#         col("Account").alias("account"),
#         col("processed_transactions.*"),
#         lit(BINANCE_EXCHANGE_NAME).alias("exchange"),
#         col("date_key"),
#         col("year_month")
#     )
#
#     print(f"table has {final_df.count()} records.")
#
#     # write_table_in_postgres(final_df, REFINED_DB, REFINED_TRADES)
#
#     return final_df
