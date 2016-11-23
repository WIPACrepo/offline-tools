
import os
import sys

files = ['/data/exp/IceCube/2013/filtered/level2/IC86_2013_GoodRunInfo.txt', '/data/exp/IceCube/2013/filtered/level2/IC86_2013_GoodRunInfo_Versioned.txt']

def read_file(f):
    fh = open(f, 'r')
    first = 2   
    r = {}
    for l in fh:
        if first > 0:
            first = first - 1
            continue
        s = l.split()
        r[s[0]] = s
    fh.close()
    return r

def cpr_grl(l1, l2):
    runs = l1.keys()
    for r in runs:
        r1 = l1[r]
        r2 = l2[r]
        for i in range(6):
            if r1[i] != r2[i]:
                print "Difference: run = %s, i = %s, values: %s <-> %s" % (r, i, r1[i], r2[i])

grl = [read_file(f) for f in files]

cpr_grl(*grl)

