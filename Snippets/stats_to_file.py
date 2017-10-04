#!/usr/bin/env python
import os,sys
import time
import json
import gzip
import cStringIO

import pymysql as MySQLdb

def compressStringToFile(filename,input):
  """
  read the given string, compress the data and write to the filename
  """
  inputFile = cStringIO.StringIO(input)
  stream = open(filename,'w')
  compressor = gzip.GzipFile(fileobj=stream, mode='w')
  while True:  # until EOF
    chunk = inputFile.read(8192)
    if not chunk:  # EOF?
      compressor.close()
      break
    compressor.write(chunk)
  stream.close()

conn = None
cursor = None

def getdbhandle():
    global conn
    global cursor
    try:
        conn = MySQLdb.connect(host = "dbs4.icecube.wisc.edu",
                       user = "i3filter_ro",
                       passwd = "Z&F7?Hu\"",
                       db = "i3filter")
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit(1)
    else:
        cursor = conn.cursor()
    
def closedb():
    global conn
    global cursor
    conn.commit()
    cursor.close()
    conn.close()



if __name__ == '__main__':

    from optparse import OptionParser
    
    parser = OptionParser()
    
    parser.add_option('-d','--dataset',dest='dataset',type='int',help='dataset to process')
    parser.add_option('-t','--type',dest='type',type='choice',choices=['task','job','dataset'],help='type of stats')
    parser.add_option('-o','--output',dest='output',default='out.json',help='output filename')
    parser.add_option('-z',dest='gzip',action='store_true',help='gzip compression')
    parser.add_option('-j',dest='bzip',action='store_true',help='bzip2 compression')
    
    (options,args) = parser.parse_args()
   
    options.output = options.output.format(dataset = options.dataset)
 
    if not options.dataset or options.dataset < 0:
        raise Exception('enter a valid dataset id')
    
    if options.output.endswith('.gz'):
        options.gzip = True
    elif options.output.endswith('.bz2'):
        options.bzip = True
    if options.gzip and options.bzip:
        raise Exception('cannot gzip and bzip at the same time. choose only one.')
    if options.gzip and not options.output.endswith('.gz'):
        options.output += '.gz'
    if options.bzip and not options.output.endswith('.bz2'):
        options.output += '.bz2'

    print "Handling: {0}".format(options.output)

    # connect to database
    getdbhandle()
    
    # get statistics
    if options.type == 'task':
        sql  = " SELECT job.queue_id,tdt.idx,tdt.iter,ts.name,ts.value "
        sql += " FROM task_statistics ts "
        sql += " JOIN task on task.task_id = ts.task_id "
        sql += " JOIN task_def_tray tdt on task.task_def_tray_id = tdt.task_def_tray_id "
        sql += " JOIN job on task.job_id = job.job_id "
        sql += " WHERE dataset_id=%s "
        bindings = (options.dataset,)
    elif options.type == 'job':
        sql  = " SELECT queue_id,name,value "
        sql += " FROM job_statistics "
        sql += " WHERE dataset_id=%s "
        bindings = (options.dataset,)
    else:
        raise Exception('bad stats type')
    cursor.execute(sql,bindings)
    results = cursor.fetchall()
    if results:
        stats = {}
        if options.type == 'task':
            for qid,idx,iter,name,value in results:
                if qid not in stats:
                    stats[qid] = {}
                if idx not in stats[qid]:
                    stats[qid][idx] = {}
                if iter not in stats[qid][idx]:
                    stats[qid][idx][iter] = {}
                stats[qid][idx][iter][name] = float(value)
        elif options.type == 'job':
            for qid,name,value in results:
                if qid not in stats:
                    stats[qid] = {}
                stats[qid][name] = float(value)
        ret = json.dumps(stats,separators=(',', ':'))
        if options.gzip:
            compressStringToFile(options.output,ret)
        else:
            open(options.output,'w').write(ret)
            if options.bzip:
                os.system('bzip2 < '+options.output+' > '+options.output+'.tmp;mv '+options.output+'.tmp '+options.output)
    else: 
        print "no statistics found for dataset", options.dataset

    closedb()
    print 'done'
