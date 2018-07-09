##############################################
##
## Calculate exponentially weighted average of cost of round-trip trade
##
## Yi Bao    06/14/2018
##
##############################################
import ccxt
import time
import datetime
import pymongo
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pprint import *
from threading import Thread
from lib_index import *

''' load exchanges '''
exchange_names = ['gdax', 'bitfinex', 'gemini', 'kraken', 'bitstamp']
exchanges = [eval('ccxt.%s()' % x) for x in exchange_names]
n_ex = len(exchange_names)
exchanges[1].rateLimit = 2000    # increase bitfinex rateLimit

symbol = 'BTC/USD'    # base/quote
C = 100
span = 1800

''' set mongodb parameters '''
host    = "gorilla0.aws.noblegrp.com"
port    = 27017
db_name = "indexation2"
query   = None

''' connect to mongo '''
try:
    db = connect_mongo(host=host, port=port, username=None, password=None, db=db_name)
    print("Connection to mongo succeeds")
except:
    print("Connection to mongo fails")

dfs = []
for i, exchange in enumerate(exchange_names):
    print(i, exchange)
    start_time = time.time()
    df = read_mongo(db, exchange, query, host = host, port = port, no_id = False)
    #df['crt'] = df.apply(lambda row: crt(row['asks'], row['bids'], C = C), axis = 1)
    #df['exp_crt'] = df['crt'].ewm(span = span, adjust = True).mean()
    df['mid'] = df.apply(lambda row: (row['asks'][0][0] + row['bids'][0][0]) / 2.0, axis = 1)
    dfs.append(df)
    end_time = time.time()
    print(end_time - start_time)

df_con = read_mongo(db, 'consolidated', query, host = host, port = port, no_id = False)
df_con['crypto_index'] = df_con.apply(lambda row: cryptoindex(row['con_ask'], row['con_bid']), axis = 1)

'''
plt.subplot()
labels = exchange_names
for i in range(len(dfs)):
    #plt.plot(dfs[i]['_id'], dfs[i]['exp_crt'], label = labels[i])
    plt.plot(dfs[i]['_id'], dfs[i]['mid'], label = labels[i])
plt.plot(df_con['_id'], df_con['crypto_index'], label = "index")
plt.xlabel('time')
plt.ylabel('BTC/USD')
plt.legend()
plt.grid()
plt.show()
'''
