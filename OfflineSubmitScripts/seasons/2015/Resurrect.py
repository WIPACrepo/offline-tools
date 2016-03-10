"""
Resurrect Subruns which have been declared bad. Exploit your evil powers.
"""

import argparse
import SQLClient_dbs4 as dbs4
import RunTools as rt
import os.path
import shutil

from glob import glob

dbs4_ = dbs4.MySQL()


if __name__ == "__main__":

    
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run',nargs="?", help="Run form which subruns shall be ressurect",type=int)
    parser.add_argument('--dataset',nargs="?", help="Dataset_id to use.",dest="dataset",type=int,default=1883)
    args = parser.parse_args()
    if args.run is None:
        print "Nothing to resurrect, specify run..."
        sys.exit(0)

    run = rt.RunTools(args.run)
    latest_prod_version = int(dbs4_.fetchall("""select production_version from grl_snapshot_info where run_id = %i""" %args.run)[0][0])
    start_date = run.GetRunTimes()["tStart"].date()
    runfiles = run.GetRunFiles(start_date,"L",ProductionVersion=latest_prod_version)
    badbasepath = os.path.join(os.path.split(runfiles[0])[0],"Bad_NotWithinGoodRunRange") 
    badfiles = glob(os.path.join(badbasepath,"*"))
    print "Found %i files in %s for production version %i, attempting to resurrect..." %(len(badfiles),badbasepath,latest_prod_version)
    goodfiles = map(lambda x: x.replace("Bad_NotWithinGoodRunRange/BadDontUse_",""),badfiles)
    print "-- moving files from %s upwards and deleting folder.." %badbasepath
    map(lambda x: shutil.move(x[0],x[1]),zip(badfiles,goodfiles))
    os.rmdir(badbasepath)
    goodfiles_nopath = map(lambda x: os.path.split(x)[1],goodfiles) # the goodfiles are the entries in urlpath
    goodfiles_db = filter(lambda x: x.endswith("bz2") or x.endswith("root"),goodfiles_nopath)
    
    # resurrect entries in urlpath table, so that files
    # get backupped to Zeuthen
    # also update job table, the run is probably "OK"
    for filename in goodfiles_db:
        print "-- -- updating urlpath and job table for %s" %filename
        dbs4_.execute("""update urlpath set transferstate="WAITING" where name="%s" and dataset_id=%i and transferstate='DELETECONFIRMED'""" %(filename,args.dataset))
        dbs4_.execute("""update job j  join urlpath u on j.queue_id=u.queue_id set j.status="OK" where u.name="%s" and u.dataset_id=%s and j.dataset_id=%s""" %(filename,args.dataset,args.dataset))

    # needs revalidation
    print "-- -- updating grl_snapshot info table validation field" 
    dbs4_.execute("""update grl_snapshot_info set validated=0 where run_id=%i""" %args.run)


