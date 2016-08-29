
from glob import glob
import os

#files = glob('/data/exp/scratch/jan/LowEn/level3/exp/201*/[0-9]*/Run00*/level3_meta.xml')
#files = glob('/data/ana/LE/level3/exp/2015/0625/Run00126514/level3_meta.xml')
files = glob('/data/ana/LE/level3/exp/201[5-6]/[0-9]*/Run00*/level3_meta.xml')

counter = 0
for file in files:
    content = ''

    with open(file, 'r') as f:
        content = f.read()

    content = content.replace('V05-00-01', 'V05-00-02')

    try:
        with open(file, 'w') as f:
            f.write(content)
    except:
        print "****ERROR WRITING %s****" % file

    counter = counter + 1

    print "(%s/%s) Updated %s" % (counter, len(files), file)

