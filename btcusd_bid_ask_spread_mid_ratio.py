################################################
## Calculate BTC/USD bid-ask spread to mid ratio
##
## Yi Bao    06/14/2018
##
################################################
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
from crt import *

''' exchanges'''
exchange_names = ['gdax', 'bitfinex', 'gemini', 'kraken', 'bitstamp']

''' set mongodb parameters '''
host    = "gorilla0.aws.noblegrp.com"
port    = 27017
db_name = "indexation"
query   = None

''' connect to mongo '''
try:
    db = connect_mongo(host=host, port=port, username=None, password=None, db=db_name)
    print("Connection to mongo succeeds")
except:
    print("Connection to mongo fails")

dfs = []
for exchange in exchange_names:
    df = read_mongo(db, exchange, query, host = host, port = port, no_id = False)
    df['mid'] = df.apply(lambda row: (row['bids'][0][0] + row['asks'][0][0]) / 2.0, axis = 1)
    df['ratio'] = df.apply(lambda row: (row['asks'][0][0] - row['bids'][0][0]) / row['mid'], axis = 1)
    dfs.append(df)

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
