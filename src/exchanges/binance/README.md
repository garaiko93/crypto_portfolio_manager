


buy trades involves (at least 2 records, 3 if fee):
["Transaction Buy"]: received coin and amount
["Transaction Spend"]: sent coin and amount
["Transaction Fee"]: fee coin and amount

    different buy operation:
["Binance Convert"]: 2 records, negative fiat and positive coin

["Buy Crypto"]: 3 records, 1 positive fiat, 1 negative fiat and 1 positive coin
    example:
        "170191496","2023-04-02 22:21:26","Spot","Buy Crypto","CHF","1962.00000000","N01339152792663436288040296"
        "170191496","2023-04-02 22:21:27","Spot","Buy Crypto","USDT","2084.81432191","N01339152792663436288040296"
        "170191496","2023-04-02 22:21:27","Spot","Buy Crypto","CHF","-1962.00000000","N01339152792663436288040296"


sell trades (at least 2 records, 3 if fee):
["Transaction Revenue"]: received coin and amount
["Transaction Sold"]: sent coin and amount
["Transaction Fee"]: fee coin and amount

deposit ops (at least 3 records, it can also be 1 record):
["Deposit"]: deposit fiat
["Transaction Related"]: fiat substract (these can be 1 second later than deposit)
["Transaction Related"]: received crypto coin
["Buy Crypto With Fiat"]: this only involves 1 record with the crypto bought
example:

        "170191496","2023-05-08 20:58:23","Spot","Buy Crypto With Fiat","USDT","2146.94444650",""

withdraw ops( 1 record):
["Withdraw"]:

staking rewards (independent):
["Staking Rewards"]: staking rewards positive value(1 record)
["Simple Earn Locked Rewards"] reward, positive value (1 record)
["Simple Earn Flexible Interest"] reward, positive value (1 record)
["Simple Earn Flexible Airdrop"]: reward, positive value (1 record)
["Launchpool Earnings Withdrawal"]: positive (1 record)
["ETH 2.0 Staking Rewards"]: eth reward in beth, positive (1 record per day)
["Distribution"]: distributions, positive (1 records)
["Cash Voucher Distribution"]:
["Cashback Voucher"]:
["Airdrop Assets"]:
["BNB Vault Rewards"]:
["Token Swap - Distribution"]: distribution of token

staking redemptions:
["Staking Redemption"] redemp staking, positive value (1 record)
["Simple Earn Locked Redemption"]: redemption, positive value (1 record)
["Simple Earn Flexible Redemption"]: redemption, positive value (1 record)

staking purchase:
["Staking Purchase"] negative value (1 record)
["Simple Earn Locked Subscription"]: negative value
["Simple Earn Flexible Subscription"]: negative value (1record)
["ETH 2.0 Staking"]: eth staking, 2 records eth and beth (2 records)

OTHERS:
["Token Swap - Redenomination/Rebranding"]: change of names of token
["Launchpool Subscription/Redemption"]: positive or negative depending (1 record)
["Asset Recovery"] ???






