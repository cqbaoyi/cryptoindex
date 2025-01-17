######################################
##
##  Library of cryptocurrency index (with API)
##  
##  Yi Bao  06/14/2018
##
######################################
from pymongo import *
import pandas as pd
import numpy as np
import bisect
import time

''' compute crypto index using snapshot of asks and bids'''
def cryptoindex(asks, bids, C = 100, D = 0.005):
    a_p = [x[0] for x in asks]
    b_p = [x[0] for x in bids]
    ''' capped order size '''
    a_v = [x[1] if x[1] <= C else C for x in asks]
    b_v = [x[1] if x[1] <= C else C for x in bids]
    ''' cumu vol '''
    cumu_a_v = np.cumsum(a_v)
    cumu_b_v = np.cumsum(b_v)
    askPV = [a_p[bisect.bisect_left(cumu_a_v, v)] if v <= cumu_a_v[-1] else a_p[-1] for v in range(1, C + 1)]
    bidPV = [b_p[bisect.bisect_left(cumu_b_v, v)] if v <= cumu_b_v[-1] else b_p[-1] for v in range(1, C + 1)]
    midPV = [(askPV[i] + bidPV[i]) / 2.0 for i in range(len(askPV))]
    midSV = [askPV[i] / bidPV[i] - 1.0 for i in range(len(askPV))]
    ''' utilized depth '''
    uti_depth = (C if D > midSV[-1] else bisect.bisect_left(midSV, D))
    uti_depth = max(uti_depth, 1.0)
    lam = 1.0 / (0.3 * uti_depth)
    ''' normalization factor '''
    exp_term = [np.exp(- lam * v) for v in range(1, C + 1)]
    NF = lam * sum(exp_term)
    ''' calculate index '''
    result = lam / NF * np.sum(np.multiply(midPV, exp_term))
    return result


''' given a list of order books (obs) and the index of exchange (i_ex), try to fetch ob '''
def _api_fetch_ob(obs, i_ex, exchange, symbol, max_order):
    try:
        ob = exchange.fetch_l2_order_book(symbol)
        ob['asks'] = ob['asks'][:max_order]    # don't want too many orders
        ob['bids'] = ob['bids'][:max_order]
        obs[i_ex] = ob
    except Exception as e:
        print(e)
        print("Continue ...")


''' API ob fetching worker '''
def api_worker(obs, i_ex, exchange, symbol, max_order = 100):
    while True:
        _api_fetch_ob(obs, i_ex, exchange, symbol, max_order)
        time.sleep(exchange.rateLimit / 1000.0)


''' make a connection to mongodb '''
def _connect_mongo(host, port, username, password, db):
    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)
    return conn[db]

''' connect to mongodb '''
''' read data and store into DataFrame '''
def read_mongo(db, collection, query={}, host='localhost', port=27017, username=None, password=None, no_id=True):
#    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)
    cursor = db[collection].find(query)
    df =  pd.DataFrame(list(cursor))
#    if no_id:
#        del df['_id']
    return df


''' cost of round-trip trade of order book'''
''' Ref: liquidity beyond the best quote: A study of the NYSE limit order book '''
def crt(asks, bids, C = 100):
    a_p = [x[0] for x in asks]
    b_p = [x[0] for x in bids]
    ''' capped order size '''
    a_v = [x[1] if x[1] <= C else C for x in asks]
    b_v = [x[1] if x[1] <= C else C for x in bids]
    ''' cumu vol '''
    cumu_a_v = np.cumsum(a_v)
    cumu_b_v = np.cumsum(b_v)
    ''' depth C may be larger than order book total cumulative v '''
    C = min(C, min(cumu_a_v[-1], cumu_b_v[-1]))
    ''' mid '''
    mid = (a_p[0] + b_p[0]) / 2.0
    ''' effective volume '''
    a_ev = []
    b_ev = []
    for i, v in enumerate(cumu_a_v):
        if C >= v:
            a_ev.append(a_v[i])
        elif i > 0:
            a_ev.append(max(0, C - cumu_a_v[i - 1]))
        else:
            a_ev.append(max(0, C))
    for i, v in enumerate(cumu_b_v):
        if C >= v:
            b_ev.append(b_v[i])
        elif i > 0:
            b_ev.append(max(0, C - cumu_b_v[i - 1]))
        else:
            b_ev.append(max(0, C))
    ''' crt '''
    crt = (np.sum(np.multiply(a_ev, np.subtract(a_p, mid))) + np.sum(np.multiply(b_ev, np.subtract(mid, b_p)))) / (C * mid)
    return crt
