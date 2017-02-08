
def get_sub_run_info(runs, db = None):
    # This function returns a dict with the run number as keys...
    # data[<RUN-NUMBER>][<SUB-RUN>][<INFORMATION>]
    # <INFORMATION>: run_id, sub_run, first_event, last_event, first_event_year, first_event_frac, last_event_year, last_event_frac, livetime

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
    
        sql = "SELECT * FROM sub_runs WHERE run_id IN (%s)" % ','.join([str(r) for r in runs])

    dbdata = db.fetchall(sql, UseDict = True)

    data = {}
    for row in dbdata:
        if row['run_id'] not in data:
            data[row['run_id']] = {}

        data[row['run_id']][row['sub_run']] = row

    return data

if __name__ == "__main__":
    print "Test script"

    print "Query info for two runs:"
    print get_sub_run_info(runs = [127390, 127391])

#    print "Query info for all runs:"
#    print get_sub_run_info(runs = 'all')
