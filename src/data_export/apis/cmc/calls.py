'''
source
https://algotrading101.com/learn/coinmarketcap-api-guide/
'''


import coinmarketcapapi
cmc = coinmarketcapapi.CoinMarketCapAPI("xxx")



data_id_map = cmc.cryptocurrency_map()
import pandas as pd
pd = pd.DataFrame(data_id_map.data, columns =['name','symbol'])
pd.set_index('symbol',inplace=True)
print(pd)

# How to get quote data using CoinMarketCap API?
data_quote = cmc.cryptocurrency_quotes_latest(symbol='ETH', convert='USD')

# How to get the latest listing data using CoinMarketCap API?
data_listing = cmc.cryptocurrency_listings_latest()
pd = pd.DataFrame(data_listing.data, columns =['name','symbol'])
pd.set_index('symbol',inplace=True)
print(pd)
len(data_listing.data)
