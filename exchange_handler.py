##########################################
##
## Use exchange APIs.
## Initialize a list of exchanges.
## Assign multithreading workers to fetch obs.
## Connect to mongodb.
## Fetch real-time obs into mongodb as 1) exchange-specific obs and 2) consolidated ob.
##
## Yi Bao    06/14/2018
##
##########################################
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

elapsed = 0
symbol = 'BTC/USD'    # base/quote
prices = []
timestamps = np.array([])

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

''' assign workers to fetch each exchange '''
obs = [{} for _ in range(n_ex)]
for i_ex in range(n_ex):
    thread = Thread(target = api_worker, args = [obs, i_ex, exchanges[i_ex], symbol])
    thread.start()

time.sleep(3)
start_time = time.time()
while elapsed < 7200:
    timestamps = np.append(timestamps, datetime.datetime.now())
    con_bid = []
    con_ask = []
    mid = []

    for i, ob in enumerate(obs):
        ob['_id'] = timestamps[-1]
        try:
            db[exchange_names[i]].insert_one(ob)
        except pymongo.errors.DuplicateKeyError:
            print("insertion of", exchange_names[i], "gives a duplicate key")
        try:
            for order in ob['bids'][:100]:
                con_bid.append(order)
            for order in ob['asks'][:100]:
                con_ask.append(order)
            mid.append((ob['asks'][0][0] + ob['bids'][0][0]) / 2.0)
        except:
            mid.append(np.nan)
    
    con_bid.sort(key = lambda x: x[0], reverse = True)
    con_ask.sort(key = lambda x: x[0])    
    index = cryptoindex(con_ask, con_bid)
    con_dict = {'con_bid': con_bid, 'con_ask': con_ask, 'index': index, '_id': timestamps[-1]}
    try:
        db['consolidated'].insert_one(con_dict)
    except pymongo.errors.DuplicateKeyError:
        print("insertion of consolidated order book gives a duplicate key")
    prices.append([index] + mid)
    print(datetime.datetime.now(), index, mid)
    elapsed += 1
    time.sleep(0.5)

end_time = time.time()
print("Elapsed time %ssecs" % (end_time - start_time))


'''
plt.subplot()
labels = ["index"] + exchange_names
prices = np.array(prices)
for i in range(len(labels)):
    plt.plot(timestamps, prices[:,i], label = labels[i])
plt.legend()
plt.grid()
plt.show()
'''
