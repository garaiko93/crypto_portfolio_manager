import argparse

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, lit, collect_list, to_json, create_map, sum

from config import REFINED_DB, REFINED_TRADES, REFINED_STAKING_REWARDS, STABLE_COINS
from utils.date_utils import get_load_date_today
from utils.spark_utils import get_spark, read_table


def get_parameteres():
    parser = argparse.ArgumentParser(description='Show portfolio analytics until specified date.')

    parser.add_argument('--datekey', help='path to study areas', default=get_load_date_today())

    args = parser.parse_args()

    return args


def process_trades(df: DataFrame, operation: str):
    if operation == "buy":
        indirect_action = "received"
        other_action = "sent"
    elif operation == "sell":
        indirect_action = "sent"
        other_action = "received"
    return (df
    .filter(col("action") == operation)
    # .filter(col(f"{other_action}_coin").isin(STABLE_COINS))
    .groupby(f"{indirect_action}_coin", f"{other_action}_coin", "exchange").agg(
        sum(f"{indirect_action}_amount").alias(f"{operation}_{indirect_action}_amount"),
        sum(f"{other_action}_amount").alias(f"{operation}_{other_action}_amount")
    )
    .groupby(f"{indirect_action}_coin", "exchange")
    .agg(
        sum(f"{operation}_{indirect_action}_amount").alias(f"{operation}_{indirect_action}_amount"),
        to_json(collect_list(create_map(f'{other_action}_coin', f'{operation}_{other_action}_amount'))).alias(f"{operation}_{other_action}_json"))
    ).sort(f"{indirect_action}_coin").select(
        col(f"{indirect_action}_coin").alias("coin"),
        col(f"{operation}_{other_action}_json"),
        col(f"{operation}_{indirect_action}_amount"),
        "exchange"
    )


def process_fees(df: DataFrame, operation: str):
    if operation == "buy":
        indirect_action = "received"
    elif operation == "sell":
        indirect_action = "sent"
    return (df.select(f"{indirect_action}_coin", f"fee_coin", f"fee_amount", "exchange")
            .filter(col(f"fee_coin").isNotNull())
            .groupby(f"{indirect_action}_coin", f"fee_coin", "exchange").agg(sum(f"fee_amount").alias(f"fee_amount"))
            .groupby(f"{indirect_action}_coin", "exchange").agg(
        to_json(collect_list(create_map(f'fee_coin', f'fee_amount'))).alias(f"{operation}_fee_json"))
            .withColumnRenamed(f"{indirect_action}_coin", "coin")
            )


if __name__ == "__main__":

    spark = get_spark("binance - refine")
    print("starting refinement")

    # args = get_parameteres()

    # read refined trades table from postgresql
    df_trades = read_table(REFINED_DB, REFINED_TRADES)
    df_staking_rewards = read_table(REFINED_DB, REFINED_STAKING_REWARDS)

    # filter by given date or take whole table
    df_trades_filtered = df_trades.filter(col("timestamp") < to_date(lit("20240406"), "yyyyMMdd"))
    df_staking_rewards_filtered = df_staking_rewards.filter(col("timestamp") < to_date(lit("20240406"), "yyyyMMdd"))
    # df_trades_filtered = df_trades.filter(col("timestamp") < args.datekey)
    # df_staking_rewards_filtered = df_trades.filter(col("timestamp") < args.datekey)

    #####################################################
    # BUY/SELL DATASETS
    #####################################################
    # group trades by coin and get current holding
    df_buys = process_trades(df_trades_filtered, "buy")
    df_sells = process_trades(df_trades_filtered, "sell")
    df_buys.show(truncate=False)
    df_sells.show(truncate=False)

    #####################################################
    # REWARDS
    #####################################################
    df_rewards = df_staking_rewards_filtered.groupby("received_coin", "exchange").agg(
        sum("received_amount").alias("reward_amount")).select(
        col("received_coin").alias("coin"),
        col("reward_amount"),
        "exchange"
    )
    df_rewards.show(truncate=False)

    #####################################################
    # PROCESS FEES INDEPENDENTLY
    #####################################################
    df_fees_buys = process_fees(df_trades_filtered, "buy")
    df_fees_sells = process_fees(df_trades_filtered, "sell")

    df_fees = df_fees_buys.join(df_fees_sells, ["coin", "exchange"], "outer")
    df_fees.show(100, truncate=False)

    # UNIQUE COINS IN PORTFOLIO
    # df_coins = (df_sells.select("coin")
    #             .union(df_buys.select("coin"))
    #             .union(df_rewards.select("coin")).distinct())

    #####################################################
    # BUILD FINAL PORTFOLIO
    #####################################################
    df_portfolio = (df_sells.drop("sell_fee_amount", "sell_fee_coin")
                    .join(df_buys.drop("buy_fee_amount", "buy_fee_coin"), ["coin", "exchange"], "outer")
                    .join(df_rewards, ["coin", "exchange"], "outer")
                    .join(df_fees, ["coin", "exchange"], "outer")
                    ).sort("coin", "exchange")
    df_portfolio.show(1000)

    # todo: review busd and usdt values, why?