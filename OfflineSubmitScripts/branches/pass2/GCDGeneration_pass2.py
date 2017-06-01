#!/usr/bin/env python

from I3Tray import *

from icecube import icetray, dataclasses, dataio
from icecube.phys_services import spe_fit_injector
from icecube.icetray import OMKey

from libs.databaseconnection import DatabaseConnection
from libs.logger import DummyLogger
import libs.files
import libs.config

if len(sys.argv)!=7:
    print "Usage: prepare_spe_corrected_gcd.py input_gcd spe_correction_file output_gcd run_id production_version snapshot_id"
    exit(1)

inputfile = sys.argv[1]
spe_file = sys.argv[2]
outputfile = sys.argv[3]
run_id = sys.argv[4]
production_version = sys.argv[5]
snapshot_id = sys.argv[6]
season = libs.config.get_season_by_run(run_id)

def to_i3_class(l):
    nl = dataclasses.I3VectorOMKey()
    for e in l:
        nl.append(e)

    return nl

def modify_bdl(frame):
    bdl = frame['BadDomsList']
    bdl_slc = frame['BadDomsListSLC']

    coxae = OMKey(19, 60)

    print "BDLs before update:"
    print "BDL:     %s" % bdl
    print "BDL SLC: %s" % bdl_slc

    bdl.append(coxae)
    bdl_slc.append(coxae)

    bdl = sorted(list(set(bdl)))
    bdl_slc = sorted(list(set(bdl_slc)))

    print "BDLs after update:"
    print "BDL:     %s" % bdl
    print "BDL SLC: %s" % bdl_slc

    del frame['BadDomsList']
    del frame['BadDomsListSLC']

    frame['BadDomsList'] = to_i3_class(bdl)
    frame['BadDomsListSLC'] = to_i3_class(bdl_slc)

tray = I3Tray()

tray.AddModule("I3Reader", "reader", filenamelist=[inputfile])

# inject the SPE correction data into the C frame
tray.AddModule(spe_fit_injector.I3SPEFitInjector, "fixspe",  Filename = spe_file)

if season > 2011 and season < 2017:
    tray.Add(modify_bdl, 'modbdl', Streams = [icetray.I3Frame.DetectorStatus])

tray.AddModule("I3Writer", filename=outputfile, CompressionLevel = 19)
tray.Execute()

sql = "UPDATE grl_snapshot_info_pass2 SET GCDCheck = 1, BadDOMsCheck = 1, PoleGCDCheck = 0, TemplateGCDCheck = 0 WHERE run_id = %s AND snapshot_id = %s AND production_version = %s" % (run_id, snapshot_id, production_version)

DatabaseConnection.get_connection('dbs4', DummyLogger()).execute(sql)

print "Done Run %s" % run_id
