#!/usr/bin/env python

import os, sys
import subprocess
import glob
import datetime
import string
import re
from dateutil.parser import parse
from dateutil.relativedelta import *
from math import *
import xml.etree.ElementTree as ET
import traceback

from dummylogger import DummyLogger

class RunTools(object):
    """
    Get files and dates for a run
    """

    def __init__(self, RunNumber, logger = DummyLogger(), passNumber = None):
        self.RunNumber = RunNumber
        self.logger = logger
        
        # Defines the number of processing. Usually it is None/pass 1. For instance when it came to the pass2 processing of 2010 - 2014, we needed pass2, so passNumber = 2.
        self.passString = ''
        self.passNumber = 1

        if passNumber is not None:
            self.passString = "pass%s" % passNumber
            self.passNumber = passNumber

        
    def GetActiveStringsAndDoms(self, Season, UpdateDB = False, gcd_file = None):
        GCDFile = gcd_file

        startDate=self.GetRunTimes()['tStart']

        detector_configuration = 'IC86' if Season > 2010 else 'IC79'

        gcd_file_glob = "/data/exp/IceCube/%s/filtered/level2%s/VerifiedGCD/Level2%s_%s.%s*%s*"%(startDate.year, self.passString, self.passString, detector_configuration, Season, self.RunNumber)

        if gcd_file is None:
            GCDFile = glob.glob(gcd_file_glob)
        
        if not len(GCDFile):
            self.logger.warning("No GCD file for run %s in Verified GCD Directory (%s) ... exiting" % (self.RunNumber, gcd_file_glob))
            return 1
        
        if gcd_file is None:
            GCDFile.sort(key=lambda x: os.path.getmtime(x))
            GCDFile = GCDFile[-1]

        from icecube import icetray, dataio, dataclasses

        f = dataio.I3File(str(GCDFile))
        BDL = None
        while f.more():
            GCD = f.pop_frame()
            if GCD.Has("BadDomsList") :
                BDL = GCD["BadDomsList"]
                break
    
        if not BDL:
            self.logger.warning("No BadDomsList object in GCD file ... exiting")
            return 1

        # Remove duplicates...IC79 has some
        self.logger.info('Bad Doms List has {} entries'.format(len(BDL)))
        initial_bdl_len = len(BDL)

        BDL = list(set(BDL))

        if len(BDL) < initial_bdl_len:
            self.logger.info('Bad Doms List length has changed due to duplicates. Removed {} DOMs.'.format(initial_bdl_len - len(BDL)))
        
        # make default configuration
        detConf = {}    
        for s in range(0,87):
            detConf[s] = range(1,67)

        # If IC79, remove some strings...
        if Season == 2010:
            self.logger.info('It\'s IC79...get rid of the IC86-only strings...')
            ic86only = [1, 7, 14, 22, 31, 79, 80]
            for s in ic86only:
                del detConf[s]
 
        # remove DOMs in BDL
        for b in BDL:
            detConf[b.string].pop(detConf[b.string].index(b.om))

        
        # count number of strings with at least 1 active non-IceTop DOM
        ActiveStrings = len([k for k in detConf.keys() if k > 0 and len(set(detConf[k]).difference([61,62,63,64,65,66]))])
        
        # add all DOMs after excluding Bad DOMs
        ActiveDoms = sum([len(set(detConf[k]).difference([65, 66])) for k in detConf.keys() if k > 0])
        
        #print [len(detConf[k]) for k in detConf.keys()]
        #for k in detConf.keys(): print detConf[k]
        
        ActiveInIceDoms = sum([len(set(detConf[k]).difference(set([61,62,63,64,65,66]))) for k in detConf.keys() if k > 0])
        
        #print [len(set(detConf[k]).difference(set([61,62,63,64]))) for k in detConf.keys()]
        
        #print ActiveStrings
        #print ActiveDoms
        #print ActiveInIceDoms

        
        if UpdateDB:
            import SQLClient_dbs4 as dbs4
            dbs4_ = dbs4.MySQL()
            #print """update i3filter.grl_snapshot_info g set ActiveStrings=%d, ActiveDoms=%s
            #                 where g.run_id=%d """%(ActiveStrings,ActiveDoms,self.RunNumber)

            passString = ''
            if self.passNumber > 1:
                passString = '_pass%s' % self.passNumber

            dbs4_.execute("""update i3filter.grl_snapshot_info%s g set ActiveStrings=%d, ActiveDoms=%s, ActiveInIceDoms=%s
                             where g.run_id=%d """%(passString, ActiveStrings,ActiveDoms,ActiveInIceDoms,self.RunNumber))
            

        
        return ActiveStrings,ActiveDoms,ActiveInIceDoms


    

    def GetRunTimes(self):
        try:
            import SQLClient_i3live as live
            m_live = live.MySQL()
        except Exception, err:
            raise Exception("Error: %s "%str(err))
        
        try:
            times_ = m_live.fetchall(""" SELECT r.tStart,r.tStart_frac,r.tStop,r.tStop_frac,
                                    r.grl_start_time,r.grl_start_time_frac,grl_stop_time,grl_stop_time_frac   
                                    from live.livedata_run r
                                    where runNumber=%s"""%(self.RunNumber),UseDict=True)
            if not len(times_):
                self.logger.warning("No time information for run = %s"%self.RunNumber)
                return []
            else:
                #print times_[0]
                return times_[0]
        except Exception, err:
            raise Exception("Error: %s "%str(err))
        
    


    def GetRunFiles(self,startDate,Type,ProductionVersion=None, season = None):
        try:
            Files = []
            
            if str(Type).upper() == "P" and self.passNumber == 2 and season in (2015, 2016):
                Files.extend(glob.glob("/data/exp/IceCube/%s/unbiased/PFDSTnoSPE/%s%s/%s*%s*"%(startDate.year,\
                                    str(startDate.month).zfill(2),str(startDate.day).zfill(2),\
                                    str(Type).upper(),self.RunNumber)))
                # useful for PFFilt files because the "spill" over multiple dates
                nextDate = startDate + relativedelta(days=1)	
                Files.extend(glob.glob("/data/exp/IceCube/%s/unbiased/PFDSTnoSPE/%s%s/%s*%s*"%(nextDate.year,\
                                    str(nextDate.month).zfill(2),str(nextDate.day).zfill(2),\
                                    str(Type).upper(),self.RunNumber)))
                
            elif str(Type).upper() == "P" and self.passNumber == 2:
                Files.extend(glob.glob("/data/exp/IceCube/%s/unbiased/PFDST/%s%s/%s*%s*"%(startDate.year,\
                                    str(startDate.month).zfill(2),str(startDate.day).zfill(2),\
                                    str(Type).upper(),self.RunNumber)))
                # useful for PFFilt files because the "spill" over multiple dates
                nextDate = startDate + relativedelta(days=1)	
                Files.extend(glob.glob("/data/exp/IceCube/%s/unbiased/PFDST/%s%s/%s*%s*"%(nextDate.year,\
                                    str(nextDate.month).zfill(2),str(nextDate.day).zfill(2),\
                                    str(Type).upper(),self.RunNumber)))
                
                # Filter _UW_ files. Those were generate in Madison, not at the pole
                Files = [f for f in Files if '_UW_' not in f]

            elif str(Type).upper() == "P" and self.passNumber == 1:
                Files.extend(glob.glob("/data/exp/IceCube/%s/filtered/PFFilt/%s%s/%s*%s*"%(startDate.year,\
                                    str(startDate.month).zfill(2),str(startDate.day).zfill(2),\
                                    str(Type).upper(),self.RunNumber)))
                # useful for PFFilt files because the "spill" over multiple dates
                nextDate = startDate + relativedelta(days=1)	
                Files.extend(glob.glob("/data/exp/IceCube/%s/filtered/PFFilt/%s%s/%s*%s*"%(nextDate.year,\
                                    str(nextDate.month).zfill(2),str(nextDate.day).zfill(2),\
                                    str(Type).upper(),self.RunNumber)))
                
            elif str(Type).upper() == "L":
                # old dir structure
                Files.extend(glob.glob("/data/exp/IceCube/%s/filtered/level2%s/%s%s/%s*%s*"%(startDate.year,\
                                    self.passString, str(startDate.month).zfill(2),str(startDate.day).zfill(2),\
                                    str(Type).upper(),self.RunNumber)))
            #    # new dir structure
                if not len(Files):
                    runnumber = self.RunNumber
                    if ProductionVersion is not None: runnumber = str(runnumber) + "_" + str(ProductionVersion)
                    Files.extend(glob.glob("/data/exp/IceCube/%s/filtered/level2%s/%s%s/Run*%s*/%s*%s*"%(startDate.year,\
                                    self.passString, str(startDate.month).zfill(2),str(startDate.day).zfill(2),\
                                    runnumber,str(Type).upper(),self.RunNumber)))
                
            else:
                self.logger.warning("""File type must be of type 'P' for PFFilt and 'L' for Level2,
                        argument type %s not recognized, returning empty list"""%str(Type))
                return Files

            if len(Files):
                Files.sort()
            return Files
            
        except Exception,err:
            raise Exception("Error: %s\n"%str(err))



    def FilesComplete(self, InFiles, RunTimes, tmppath = '', showTimeMismatches = True, outdict = None, no_xml_bundle = False):
        error = False

        def get_file_part(f):
            if no_xml_bundle:
                return int(f.split('Subrun00000000_')[1].split('.i3')[0])
            else:
                if 'Subrun' in f:
                    return int(re.sub("[^0-9]", "" ,f.split("Subrun")[1].split(".bz2")[0]))
                elif 'Part' in f:
                    return int(re.sub("[^0-9]", "", f.split("Part")[1].split(".bz2")[0]))
                else:
                    raise Exception('Do not understand file name: {}'.format(f))

        try:
            if outdict is not None:
                outdict[self.RunNumber] = {'missing_files': [], 'metadata_start_time': None, 'metadata_stop_time': None, 'metadata_start_time_error': False, 'metadata_stop_time_error': False}

            if not len(InFiles):
                self.logger.warning( "Input file list is empty, maybe failed/short run")
                return 0
            
            InFiles.sort(key = get_file_part)

            FileParts = [get_file_part(f) for f in InFiles]
            FileParts.sort()

            if not len(FileParts) or max(FileParts)>1000:
                self.logger.warning( "Could not resolve number of files from file names ... file names probably do not follow expected naming convention")
                return 0
            
            self.logger.debug("File parts = %s" % FileParts)

            if len(InFiles) != FileParts[-1] + 1:
                self.logger.warning( "Looks like we don't have all the files for this run")
                MissingParts = (set(range(0,FileParts[-1] + 1))).difference(set(FileParts))
                self.logger.warning("There are %s Missing Part(s):" % len(MissingParts))
                MissingParts = list(MissingParts)
                MissingParts.sort()
                for m in MissingParts:
                    self.logger.warning(m)

                if outdict is not None:
                    outdict[self.RunNumber]['missing_files'] = MissingParts

                error = True

            RunStart = RunTimes['tStart']
            if isinstance(RunTimes['grl_start_time'],datetime.datetime): RunStart = RunTimes['grl_start_time']
                
            RunStop = RunTimes['tStop']
            if isinstance(RunTimes['grl_stop_time'],datetime.datetime): RunStop = RunTimes['grl_stop_time']
                
            if not no_xml_bundle:
                tmpfile = os.path.join(tmppath, 'tmp.xml')
     
                self.logger.info('Check start time')
    
                StartCheck = 0
                processoutput = subprocess.check_output("""tar xvf %s --exclude="*.i3*" -O > %s""" % (InFiles[0], tmpfile), shell = True, stderr=subprocess.STDOUT)
                self.logger.debug(processoutput.strip())
    
                doc = ET.ElementTree(file = tmpfile)
                sTime = [t.text for t in doc.getiterator() if t.tag=="Start_DateTime"]
                fs_time = parse(sTime[0])
                
                self.logger.info('Check stop time')
    
                EndCheck = 0
                processoutput = subprocess.check_output("""tar xvf %s --exclude="*.i3*" -O > %s""" % (InFiles[-1], tmpfile), shell = True, stderr=subprocess.STDOUT)
                self.logger.info(processoutput.strip())
                
                doc = ET.ElementTree(file = tmpfile)
                eTime = [t.text for t in doc.getiterator() if t.tag=="End_DateTime"]
                fe_time = parse(eTime[0])
                
                #t_diff = RunStop - fe_time
                #t_diff_s = abs((t_diff.seconds*I3Units.s + (t_diff.microseconds*I3Units.microsecond) + t_diff.days*I3Units.day)/I3Units.s)
                #EndCheck = int(t_diff_s < 1.0)
            else:
                self.logger.info('Check start time')
                fs_time = self._get_start_time_of_file(InFiles[0])

                self.logger.info('Check stop time')
                fe_time = self._get_stop_time_of_file(InFiles[-1])
 
            StartCheck = fs_time <= RunStart
            EndCheck = fe_time >= RunStop

            if not StartCheck and showTimeMismatches:
                self.logger.warning( "mismatch in start time reported by i3Live:%s and file metadata:%s"%(RunStart, fs_time)) 
                outdict[self.RunNumber]['metadata_start_time_error'] = True

            if not EndCheck and showTimeMismatches:
                self.logger.warning( "mismatch in end time reported by i3Live:%s and file metadata:%s"%(RunStop, fe_time))        
                outdict[self.RunNumber]['metadata_stop_time_error'] = True

            if outdict is not None:
                outdict[self.RunNumber]['metadata_start_time'] = fs_time
                outdict[self.RunNumber]['metadata_stop_time'] = fe_time
            
            return int(not error) * StartCheck * EndCheck
          
        except Exception, err:
            self.logger.error("FilesComplete Error: %s\n"%str(err))
            traceback.print_exc()
            return 0

    def _get_start_time_of_file(self, f):
        from icecube import dataio, dataclasses
        i3file = dataio.I3File(f)

        while i3file.more():
            frame = i3file.pop_frame()

            if 'I3EventHeader' in frame.keys():
                t = frame['I3EventHeader'].start_time.date_time
                i3file.close()
                return t.replace(microsecond = 0)

        i3file.close()

        raise RuntimeError('Cannot determin start time of file.')

    def _get_stop_time_of_file(self, f):
        from icecube import dataio, dataclasses
        i3file = dataio.I3File(f)

        time = None
        while i3file.more():
            frame = i3file.pop_frame()

            if 'I3EventHeader' in frame.keys():
                time = frame['I3EventHeader'].start_time.date_time

        i3file.close()

        if time is None:
            raise RuntimeError('Cannot determin start time of file.')
        else:
            return time.replace(microsecond = 0)

