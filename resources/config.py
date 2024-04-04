import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__)).split("resources")[0]

# inputCsvPath = "data/raw/binance/csvByMonth/"
RAW_DATA_PATH = f"{ROOT_DIR}/data/raw"
EXCHANGES_RAW_FILES = f"{RAW_DATA_PATH}/exchanges"

# BINANCE VARIABLES
BINANCE_RAW_PATH = f"{EXCHANGES_RAW_FILES}/binance"
BINANCE_CSV_PATH = f"{BINANCE_RAW_PATH}/csv"

##################################################################
# POSTGRESQL DATABASE
##################################################################
RAW_DB = "raw"
REFINED_DB = "refined"
CURATED_DB = "curated"

# BINANCE
BINANCE_EXCHANGE_NAME = "binance"
BINANCE_RAW_TABLE = "binance_record_history_raw"

# SWISSBORG
SWISSBORG_EXCHANGE_NAME = "swissborg"
SWISSBORG_RAW_TABLE = "swissborg_record_history_raw"

# KUCOIN
KUCOIN_EXCHANGE_NAME = "kucoin"
KUCOIN_RAW_TABLE = "kucoin_record_history_raw"


# refined tables
REFINED_TRADES = "trades"
REFINED_STAKING_REWARDS = "staking_rewards"



##################################################################
# BINANCE VARIABLES
##################################################################

BINANCE_TRADE_OPS = ["Transaction Buy",
                     "Transaction Spend",
                     "Transaction Fee",
                     "Transaction Sold",
                     "Transaction Revenue"]

BINANCE_STAKING_REWARDS_OPS = [
    "Staking Rewards",
    "Simple Earn Locked Rewards",
    "Simple Earn Flexible Interest",
    "Simple Earn Flexible Airdrop",
    "Launchpool Earnings Withdrawal",
    "ETH 2.0 Staking Rewards",
    "Distribution",
    "Cash Voucher Distribution",
    "Cashback Voucher",
    "Airdrop Assets",
    "BNB Vault Rewards",
    "Token Swap - Distribution"
]

BINANCE_OPS = {'trade_ops': ["Transaction Buy",
                             "Transaction Spend",
                             "Transaction Fee",
                             "Transaction Sold",
                             "Transaction Revenue"],
               'staking_rewards_ops': [
                   "Staking Rewards",
                   "Simple Earn Locked Rewards",
                   "Simple Earn Flexible Interest",
                   "Simple Earn Flexible Airdrop",
                   "Launchpool Earnings Withdrawal",
                   "ETH 2.0 Staking Rewards",
                   "Distribution",
                   "Cash Voucher Distribution",
                   "Cashback Voucher",
                   "Airdrop Assets",
                   "BNB Vault Rewards",
                   "Token Swap - Distribution"
               ],
               'staking_redemption_ops': [
                   "Staking Redemption",
                   "Simple Earn Locked Redemption",
                   "Simple Earn Flexible Redemption"
               ],
               'staking_purchase_ops': [
                   "Staking Purchase",
                   "Simple Earn Locked Subscription",
                   "Simple Earn Flexible Subscription",
                   "ETH 2.0 Staking"
               ],
               'deposit_ops': [
                   "Transaction Related",
                   "Deposit"
               ],
               'withdraw_ops': [
                   "Withdraw"
               ],
               'other_ops': [
                   "Token Swap - Redenomination/Rebranding",
                   "Launchpool Subscription/Redemption",
                   "Asset Recovery"
               ]
               }

##################################################################
# OTHER VARIABLES
##################################################################
STABLE_COINS = ["USDT", "BUSD"]




# BINANCE_OPS = {'trade_ops': ["Transaction Buy",
#                              "Transaction Sold"
#                              "Sell",
#                              "Large OTC trading"],
#                'interest_ops': [
#                    "ETH 2.0 Staking Rewards",
#                    # "ETH 2.0 Staking",
#                    "POS savings interest",  # Staking bloquead ???
#                    # "POS savings purchase"
#                    "Launchpool Interest",
#                    "Savings Interest",  # Earn flexible interest
#                    "Super BNB Mining"
#                    # "Savings purchase"]
#                ],
#                'deposit_ops': [
#                    "Transaction Related",
#                    "Deposit"
#                ],
#                'withdraw_ops': ["Withdraw"],
#                'saving_redemption_ops': [
#                    "Savings Principal redemption",
#                    "POS savings redemption"
#                ]
#                }
