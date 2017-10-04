
import os
import sys

files = [{
            'file': '/data/exp/IceCube/2013/filtered/level2/IC86_2013_GoodRunInfo.txt',
            'skip_first_lines': 2
         },
         {
            'file': '/data/exp/IceCube/2013/filtered/level2pass2/IC86_2013_GoodRunInfo.txt',
            'skip_first_lines': 2
         }]

def read_file(f, skip_first_lines = 2):
    fh = open(f, 'r')
    first = skip_first_lines
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
    runs = sorted(list(set(l1.keys()) & set(l2.keys())))
    for r in runs:
        r1 = l1[r]
        r2 = l2[r]
        for i in range(9):
            if i < len(r1) and i < len(r2):
                if r1[i] != r2[i]:
                    print "Difference: run = %s, i = %s, values: %s <-> %s" % (r, i, r1[i], r2[i])
            elif i < len(r1):
                print "Difference: run = %s, i = %s, values: %s <-> %s" % (r, i, r1[i], '<EMPTY>')
            elif i < len(r2):
                print "Difference: run = %s, i = %s, values: %s <-> %s" % (r, i, '<EMPTY>', r2[i])

    print ''

    missing_in_l1 = list(set(l2) - set(l1))
    missing_in_l2 = list(set(l1) - set(l2))

    if len(missing_in_l1):
        print 'Missing runs in l1:'
        for r in missing_in_l1:
            print r

    if len(missing_in_l2):
        print 'Missing runs in l2:'
        for r in missing_in_l2:
            print r

if __name__ == "__main__":
    grl = [read_file(f['file'], f['skip_first_lines']) for f in files]
    cpr_grl(*grl)

