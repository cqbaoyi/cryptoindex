# Ref: Liquidity beyond the best quote: A study of the NYSE limit order book 

import numpy as np

''' cost of round-trip trade of order book'''
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
