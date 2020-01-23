import os
import numpy as np 
import pandas as pd 
import pickle
import quandl
from datetime import datetime 

import plotly.offline as ply 
import plotly.graph_objs as pgo 
import plotly.figure_factory as pff 
ply.init_notebook_mode(connected=True)

#API key for Quandl
quandl.ApiConfig.api_key = "APIKEYGOESHERE"
#Pull data from quandl API, pickle is used serialize data so it does not need to be redownloaded.
def pull_quandl_data(quandl_id):
    cache_path = '{}.pk1'.format(quandl_id).replace('/','-')
    try: #Will run successfully if pk file has already been created.
        f = open(cache_path, 'rb')
        df = pickle.load(f)  
        print('Loaded from cache' .format(quandl_id))
    except(OSError, IOError) as error: #will run when Pk file does not exist, and download then create new pk file from df
        print('Downloading {} from Quandl' .format(quandl_id))
        df=quandl.get(quandl_id, returns="pandas")
        df.to_pickle(cache_path)
        print('Cached {} at {}' .format(quandl_id, cache_path))
    return df 

# BTC price data pull from Kraken
kraken_btc_price_usd = pull_quandl_data('BCHARTS/KRAKENUSD')

#check first 5 rows of df
kraken_btc_price_usd.head()

#Chart for BTC price data
btc_chart = pgo.Scatter(x=kraken_btc_price_usd.index, y=kraken_btc_price_usd['Weighted Price'])
ply.iplot([btc_chart])

#Add price data from other BTC exchanges
exchanges = ['Coinbase', 'Bitstamp', 'Cbx', 'ITBIT'] # see https://blog.quandl.com/api-for-bitcoin-data for a full list.
exchange_data = {}
exchange_data['Kraken'] = kraken_btc_price_usd

#add the rest of exchanges to exchange_data
for ex in exchanges:
    ex_code = 'BCHARTS/{}USD'.format(ex)
    btc_exchange_df = pull_quandl_data(ex_code)  #pulls quandl data and saves the df for each exchange via pickle
    exchange_data[ex] = btc_exchange_df


#Function to merge all pricing data into one Dataframe from selected columns
def merge_df_columns(dataframes, labels, col):
    merged_dict = {}

    for i in range(len(dataframes)):
        merged_dict[labels[i]] = dataframes[i][col]

    return pd.DataFrame(merged_dict)

#Merging the BTC price dataframes together using the 'Weighted Price' Column
btc_datasets_usd = merge_df_columns(list(exchange_data.values()), list(exchange_data.keys()), 'Weighted Price')
