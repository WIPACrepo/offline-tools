
def get_sub_run_info(runs, db = None, nice_times = False):
    # This function returns a dict with the run number as keys...
    # data[<RUN-NUMBER>][<SUB-RUN>][<INFORMATION>]
    # <INFORMATION>: run_id, sub_run, first_event, last_event, first_event_year, first_event_frac, last_event_year, last_event_frac, livetime

    if nice_times:
        from icecube import dataclasses

    if db is None:
        import sys
        sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2016/')
        from databaseconnection import DatabaseConnection
        from libs.logger import DummyLogger

        db = DatabaseConnection.get_connection('filter-db', DummyLogger())

    if runs is None:
        print "At least one run is required."
        return

    if runs == 'all':
        sql = "SELECT * FROM sub_runs"
    else:
        if not isinstance(runs, list):
            runs = [runs]
    
        if len(runs) == 0:
            print "At least one run is required."
            return
    
        sql = "SELECT * FROM sub_runs WHERE run_id IN (%s) ORDER BY run_id, sub_run" % ','.join([str(r) for r in runs])

    dbdata = db.fetchall(sql, UseDict = True)

    data = {}
    for row in dbdata:
        if row['run_id'] not in data:
            data[row['run_id']] = {}

        if nice_times:
            row['first_event'] = dataclasses.I3Time(row['first_event_year'], row['first_event_frac'])
            row['last_event'] = dataclasses.I3Time(row['last_event_year'], row['last_event_frac'])

        data[row['run_id']][row['sub_run']] = row

    return data

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('--nice-times', help = "Make times readable", action = "store_true", default = False)
    parser.add_argument('--only-start-stop', help = "Print only start/stop time", action = "store_true", default = False)
    parser.add_argument('runid', nargs = '+', help = "Run ids", type = int)
    args = parser.parse_args()

    data = get_sub_run_info(runs = args.runid, nice_times = args.nice_times)

    if args.only_start_stop:
        for run_id, files in data.items():
            file_ids = files.keys()

            print 'file #{0}: {1}'.format(file_ids[0], files[file_ids[0]]['first_event'])
            print 'file #{0}: {1}'.format(file_ids[-1], files[file_ids[-1]]['last_event'])
    else:
        print data

