#!/usr/bin/env python

import os,sys
import json
import subprocess as sub
import glob
from operator import add
import cPickle
import datetime



def parseFiles(jFiles, outFile):

    dates_ = []
    allDataDict = {}
    countFiles = 0

    for j in jFiles:
        print j
        try:
            tmpRecords = (json.load(open(j,'r')))
            date_ = tmpRecords.keys()[0]
            entries_ = tmpRecords[tmpRecords.keys()[0]]
            dRecords = []
            if len(entries_.keys()) == 1:
                dRecords = entries_[entries_.keys()[0]]
            else:
                dRecords_ = []
                for k in entries_.keys(): dRecords_.extend(entries_[k])
    
                dictRecords = {}
                for d in dRecords_:
                    if dictRecords.has_key(d[0]+"+"+d[1]):
                        dictRecords[d[0]+"+"+d[1]]+=d[2]
                    else:
                        dictRecords[d[0]+"+"+d[1]] = d[2]
    
                for k in dictRecords.keys():
                    trec = k.split("+")
                    trec.append(dictRecords[k])
                    dRecords.append(trec)
    

            dates_.append(str(date_).replace("-","/") )
    
            for d in dRecords:
                
                # hack needed because of possible multiple identifiers for users after splitting on '+"
                if len(d)>3 : d = [d[0],"_".join(d[1:-1]),d[-1]]
                
                # switch name when defualt account is left empty, this is the case with CHTC
                if d[0] == '<none>': d[0] = u'default(none)'
                
                # add new accounting group
                if not allDataDict.has_key(d[0]):
                    allDataDict[d[0]] = []
                    allDataDict[d[0]].append([d[1]])
    
    
                # check if user name already exists in accounting group
                # add user name and usage where it does not exist
                # zero pad beginning of entry to match length of already existing records
                tmp = [a for a in allDataDict[d[0]] if d[1] in a]

                if not len(tmp):
                    tmp1 = [d[1]]
                    if countFiles:tmp1.extend([0]*(countFiles))
                    tmp1.append(d[2])
                    allDataDict[d[0]].append(tmp1)
    
                # just add usage where username already exists
                else:
                    tmp = tmp[0]
                    pos_ = allDataDict[d[0]].index(tmp)
                    # zero pad in from for new name entries
                    tmp.extend([0]*(1-(len(tmp)-countFiles)))
                    tmp.append(d[2])
                    allDataDict[d[0]][pos_] = tmp
        
    
            # zero pad exisiting user names that don't have entries for this daily record
            for k in allDataDict.keys():
                for a in allDataDict[k]:
                    # <2 implies no entry was made for this exisiting record
                    if len(a)-countFiles<2:
                        a.append(0)
                          
    
    
            countFiles+=1
        except Exception,err:
            print "Error from file %s: "%j,str(err)
            pass


    ## separate results into cpu and gpu accounts
    gpuDataDict = {}
    cpuDataDict = {}
    for k in allDataDict.keys():
        if "gpu" in k:
            gpuDataDict[k] = allDataDict[k]
        else:
            cpuDataDict[k] = allDataDict[k]


    #
    #kkk = cpuDataDict.keys()
    #for kk in kkk:
    #    ppl = cpuDataDict[kk]
    #    
    #    for pl in ppl :
    #        print pl[0:2], isinstance(pl[2],(int,float))
    #        if not isinstance(pl[2],(int,float)) : del ppl[pl]
    #
    #raise "*"

    gAcctDataDict = {}
    gData = []
    for k in gpuDataDict.keys(): gAcctDataDict[k] = []
    for c in range(countFiles):
        for t in gpuDataDict.keys():
            tmpData = [r for r in gpuDataDict[t]]
            if tmpData[0] not in gData : gData.extend([r for r in gpuDataDict[t]])
            gAcctDataDict[t].append(sum([r[c+1] for r in gpuDataDict[t]]))
           
    # convert to sorted list to be read directly by js
    gAcctDataList = convertToList(gAcctDataDict)


    gNameDataDict = {}
    for ad in gData:
        if gNameDataDict.has_key(ad[0]): 
            gNameDataDict[ad[0]] = map(add,[a if a is not None else 0 for a in ad[1:]],[b if b is not None else 0 for b in gNameDataDict[ad[0]]])
        else:
            gNameDataDict[ad[0]] = ad[1:]
                    
    gNameDataList = convertToList(gNameDataDict)
    
    
    cAcctDataDict = {}
    cData = []
    for k in cpuDataDict.keys(): cAcctDataDict[k] = []
    for c in range(countFiles):
        for t in cpuDataDict.keys():
            tmpData = [r for r in cpuDataDict[t] ]
            if tmpData[0] not in cData : cData.extend([r for r in cpuDataDict[t]])
            cAcctDataDict[t].append(sum([r[c+1] for r in cpuDataDict[t] ]))

    # convert to sorted list to be read directly by js
    cAcctDataList = convertToList(cAcctDataDict)


    cNameDataDict = {}
    for ad in cData:
        if cNameDataDict.has_key(ad[0]):
            cNameDataDict[ad[0]] = map(add,[a for a in ad[1:]],[b for b in cNameDataDict[ad[0]]])
        else:
            cNameDataDict[ad[0]] = ad[1:]

    cNameDataList = convertToList(cNameDataDict)


    with open(outFile,"wb") as f:
        cPickle.dump([dates_,cAcctDataList,cNameDataList,gAcctDataList,gNameDataList],f,protocol=cPickle.HIGHEST_PROTOCOL)

    #return allDataDict,countFiles


def convertToList(Dict_):
    List_ = []
    for d in Dict_:
        tmp = [d]
        tmp.extend(Dict_[d])
        tmp.extend([sum(Dict_[d]),100.*sum(Dict_[d])/sum([sum(Dict_[k]) for k in Dict_])])
        List_.append(tmp)
                  
        #List_.sort(key=lambda x: x[-1], reverse=True)
        List_.sort(key=lambda x: x[-1])
                        
    return List_



CheckTime = datetime.datetime.now()
print "\n====== Checking for updates at ",CheckTime," ======"

workingDirs = {'NPX':'/net/icecube-usr/i3filter','CHTC':'/net/icecube-usr/i3filter/chtc'}
#workingDirs = {'NPX':'/net/icecube-usr/i3filter'}
#workingDirs = {'CHTC':'/net/icecube-usr/i3filter/chtc'}

for w in workingDirs.keys():
    print "\nprocessing ", w,workingDirs[w]
    jAllFiles = glob.glob(workingDirs[w]+'/*.json')
    #jAllFiles.sort(key=lambda x: os.path.getmtime(x))
    jAllFiles.sort(key=lambda x: os.path.basename(x))

    jFiles = jAllFiles[-7:]
    outFile = "/net/icecube-usr/i3filter/%sCondorUsage_LastWeek.dat"%w
    print "Writing weekly results to ",outFile
    parseFiles(jFiles,outFile)
    
    jFiles = jAllFiles
    outFile = "/net/icecube-usr/i3filter/%sCondorUsage.dat"%w
    print "Writing long-term results to ",outFile
    parseFiles(jFiles,outFile)

