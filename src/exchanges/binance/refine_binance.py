from pyspark.sql.functions import col

from config import BINANCE_RAW_TABLE, RAW_DB, BINANCE_STAKING_REWARDS_OPS, SWISSBORG_RAW_TABLE, \
    KUCOIN_RAW_TABLE
from exchanges.binance.process_staking_rewards import refine_staking_rewards
from utils.spark_utils import read_table, get_spark

def refine_binance():
    df_raw_binance = read_table(RAW_DB, BINANCE_RAW_TABLE)

    # process trades - buy and sells
    # df_raw_binance_trades = df_raw_binance.filter(col("Operation").isin(BINANCE_TRADE_OPS))
    # df_refined_binance_trades = refine_binance_trades(df_raw_binance_trades)

    # process staking rewards
    df_raw_staking = df_raw_binance.filter(col("Operation").isin(BINANCE_STAKING_REWARDS_OPS))
    df_refined_staking_rewards = refine_staking_rewards(df_raw_staking)

    # return df_refined_binance_trades, df_refined_staking_rewards

    return None, df_refined_staking_rewards
    # return df_refined_binance_trades, None


if __name__ == "__main__":

    spark = get_spark("binance - refine")
    print("starting refinement")
    
    # load raw dataframes
    df_raw_binance = read_table(RAW_DB, BINANCE_RAW_TABLE)
    df_raw_swissborg = read_table(RAW_DB, SWISSBORG_RAW_TABLE)
    df_raw_kucoin = read_table(RAW_DB, KUCOIN_RAW_TABLE)

    refined_binance_trades, refined_binance_staking_rewards = refine_binance()
    # refined_binance_trades.show()
    refined_binance_staking_rewards.show()

    # df_trades = refined_binance_trades.union()




