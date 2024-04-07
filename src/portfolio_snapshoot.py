import argparse

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, lit, collect_list, to_json, create_map

from config import REFINED_DB, REFINED_TRADES, REFINED_STAKING_REWARDS, STABLE_COINS
from utils.date_utils import get_load_date_today
from utils.spark_utils import get_spark, read_table


def get_parameteres():
    parser = argparse.ArgumentParser(description='Show portfolio analytics until specified date.')

    parser.add_argument('--datekey', help='path to study areas', default=get_load_date_today())

    args = parser.parse_args()

    return args


if __name__ == "__main__":

    spark = get_spark("binance - refine")
    print("starting refinement")

    args = get_parameteres()

    # read refined trades table from postgresql
    df_trades = read_table(REFINED_DB, REFINED_TRADES)
    df_staking_rewards = read_table(REFINED_DB, REFINED_STAKING_REWARDS)

    # filter by given date or take whole table
    df_trades_filtered = df_trades.filter(col("timestamp") < to_date(lit("20240406"), "yyyyMMdd"))
    df_staking_rewards_filtered = df_staking_rewards.filter(col("timestamp") < to_date(lit("20240406"), "yyyyMMdd"))
    # df_trades_filtered = df_trades.filter(col("timestamp") < args.datekey)
    # df_staking_rewards_filtered = df_trades.filter(col("timestamp") < args.datekey)

    # group trades by coin and get current holding
    def process_trades(df: DataFrame, operation: str):
        if operation == "buy":
            indirect_action = "received"
            other_action = "sent"
        elif operation == "sell":
            indirect_action = "sent"
            other_action = "received"
        return (df
        .filter(col("action") == operation)
        .filter(col(f"{other_action}_coin").isin(STABLE_COINS))
        .groupby(f"{indirect_action}_coin", f"{other_action}_coin").agg(
            sum(f"{indirect_action}_amount").alias(f"{operation}_{indirect_action}_amount"),
            sum(f"{other_action}_amount").alias(f"{operation}_{other_action}_amount")
        )
        .groupby(f"{indirect_action}_coin")
        .agg(
            sum(f"{operation}_{indirect_action}_amount").alias(f"{operation}_{indirect_action}_amount"),
            to_json(collect_list(create_map(f'{other_action}_coin', f'{operation}_{other_action}_amount'))).alias(f"{operation}_{other_action}_json"))
        ).sort(f"{indirect_action}_coin").select(
            col(f"{indirect_action}_coin").alias("coin"),
            col(f"{operation}_{other_action}_json"),
            col(f"{operation}_{indirect_action}_amount")
        )

    df_buys = process_trades(df_trades_filtered, "buy")
    df_sells = process_trades(df_trades_filtered, "sell")

    # df_buys = (df_trades_filtered
    # .filter(col("action") == "buy")
    # .filter(col("sent_coin").isin(STABLE_COINS))
    # .groupby("sent_coin", "fee_coin", "received_coin")
    # .agg(
    #     sum("sent_amount").alias("sent_amount"),
    #     sum("fee_amount").alias("fee_amount"),
    #     sum("received_amount").alias("received_amount"),
    # )
    # .sort("received_coin").select(
    #     col("received_coin").alias("coin"),
    #     col("sent_coin").alias("buy_sent_coin"),
    #     col("fee_coin").alias("buy_fee_coin"),
    #     col("sent_amount").alias("buy_sent_amount"),
    #     col("received_amount").alias("buy_received_amount"),
    #     col("fee_amount").alias("buy_fee_amount"),
    # ))
    # df_sells = (df_trades_filtered
    # .filter(col("action") == "sell")
    # .filter(col("received_coin").isin(STABLE_COINS))
    # .groupby("sent_coin", "fee_coin", "received_coin")
    # .agg(
    #     sum("sent_amount").alias("sent_amount"),
    #     sum("fee_amount").alias("fee_amount"),
    #     sum("received_amount").alias("received_amount"))
    # .sort("sent_coin").select(
    #     col("sent_coin").alias("coin"),
    #     col("received_coin").alias("sell_received_coin"),
    #     col("fee_coin").alias("sell_fee_coin"),
    #     col("sent_amount").alias("sell_sent_amount"),
    #     col("received_amount").alias("sell_received_amount"),
    #     col("fee_amount").alias("sell_fee_amount")
    # ))

    df_rewards = df_staking_rewards_filtered.groupby("received_coin").agg(
        sum("received_amount").alias("reward_amount")).select(
        col("received_coin").alias("coin"),
        col("reward_amount")
        # col("exchange")
    )
    def process_fees(df: DataFrame, operation: str):
        return (df.select("coin", f"{operation}_fee_coin", f"{operation}_fee_amount")
        .filter(col("buy_fee_coin").isNotNull())
        .groupby("coin", f"{operation}_fee_coin").agg(sum(f"{operation}_fee_amount").alias(f"{operation}_fee_amount"))
        .groupby("coin").agg(
            to_json(collect_list(create_map(f'{operation}_fee_coin', f'{operation}_fee_amount'))).alias(f"{operation}_fee_json"))
        )

    # PROCESS FEES INDEPENDENTLY
    df_fees_buys = process_fees(df_buys, "buy")
    df_fees_sells = process_fees(df_sells, "sell")

    df_fees = df_fees_buys.join(df_fees_sells, "coin", "outer")
    df_fees.show(100, truncate=False)

    # UNIQUE COINS IN PORTFOLIO
    df_coins = (df_sells.select("coin")
                .union(df_buys.select("coin"))
                .union(df_rewards.select("coin")).distinct())




    # BUILD FINAL PORTFOLIO
    df_portfolio = (df_coins
                    .join(df_sells.drop("", "sell_fee_amount", "sell_fee_coin"), ["coin"], "left")
                    .join(df_buys.drop("buy_fee_amount", "buy_fee_coin"), ["coin"], "left")
                    .join(df_rewards, ["coin"], "left")
                    .join(df_fees, ["coin"], "left")
                    ).sort("coin")
    df_portfolio.show(1000)

    # group fees by received key (ideally convert to usd)
    #