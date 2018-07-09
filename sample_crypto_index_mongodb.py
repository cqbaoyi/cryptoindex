########################################
##
## This script is obsolete.
##
## Yi Bao    06/14/2018
##
########################################
from pymongo import MongoClient
import numpy as np
import pandas as pd
import bisect
import pprint

''' make a connection to mongodb '''
def _connect_mongo(host, port, username, password, db):
    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)
    return conn[db]


''' read mongo and store into DataFrame '''
def read_mongo(db, collection, query={}, host='localhost', port=27017, username=None, password=None, no_id=True):
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)
    cursor = db[collection].find(query)
    df =  pd.DataFrame(list(cursor))
#    if no_id:
#        del df['_id']
    return df


''' calculate cryptocurrency index for each time stamp using BRTI methodology'''
def cryptoindex(row, C = 100, D = 0.005):
    ''' exclude NaN rows'''
    if not np.isnan(row['traded_price']):
        return np.nan
    a_p = row['ask_price']
    b_p = row['bid_price']
    ''' capped order size '''
    a_v = [x if x <= C else C for x in row['ask_volume']]
    b_v = [x if x <= C else C for x in row['bid_volume']]
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


''' main function '''
host = "gorilla0.aws.noblegrp.com"
port = 27017
db   = "test"
collection = "gdaxbtcusd"
query = None
''' order book dataframe'''
df_ob = read_mongo(db, collection, query, host = host, port = port, no_id = True)
df_ob['ask_price']  = df_ob['ask_price'].apply(lambda row: [float(x) for x in row])
df_ob['bid_price']  = df_ob['bid_price'].apply(lambda row: [float(x) for x in row])
df_ob['ask_volume'] = df_ob['ask_volume'].apply(lambda row: [float(x) for x in row])
df_ob['bid_volume'] = df_ob['bid_volume'].apply(lambda row: [float(x) for x in row])
df_ob['ask']        = df_ob['ask_price'].apply(lambda row: row[0])
df_ob['bid']        = df_ob['bid_price'].apply(lambda row: row[0])
df_ob['mid']        = df_ob[['ask', 'bid']].mean(axis=1)
exit(1)
''' trade dataframe '''
collection = "gdaxbtcusd_trade"
df_trade = read_mongo(db, collection, query, host = host, port = port, no_id = True)
df_trade = df_trade.rename(columns = {'ask_price': 'ask', 'bid_price': 'bid'})
''' combine dataframes '''
df = pd.concat([df_ob, df_trade])
df = df.sort_values(by = ['datetime'])
df = df.reset_index(drop=True)

### calculate index
C = 100        # order size cap
D = 0.005      # deviation from mid
df['NCI_crypto_index'] = df.apply(lambda row: cryptoindex(row, C, D), axis = 1)
df.to_csv('index.csv', columns = ['datetime', 'ask', 'bid', 'mid', 'traded_price', 'NCI_crypto_index'])
