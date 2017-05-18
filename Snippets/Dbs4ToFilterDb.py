
import sys

sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2016/')
from libs.databaseconnection import DatabaseConnection
from libs.logger import DummyLogger

def grl_to_runs(dbs4, filter_db):
    dbs4_sql = 'SELECT * FROM i3filter.grl_snapshot_info g JOIN i3filter.run_info_summary s ON g.run_id = s.run_id ORDER BY g.run_id, snapshot_id'
    
    filter_db_sql = '''INSERT INTO `i3filter`.`runs`
    (`run_id`,
    `snapshot_id`,
    `production_version`,
    `good_i3`,
    `good_it`,
    `reason_i3`,
    `reason_it`,
    `good_tstart`,
    `good_tstart_frac`,
    `good_tstop`,
    `good_tstop_frac`,
    `tstart`,
    `tstart_frac`,
    `tstop`,
    `tstop_frac`,
    `nevents`,
    `rate`,
    `gcd_generated`,
    `gcd_bad_doms_validated`,
    `gcd_pole_validation`,
    `gcd_template_validation`,
    `active_strings`,
    `active_doms`,
    `active_in_ice_doms`)
    VALUES
    ({run_id},
    {snapshot_id},
    {production_version},
    {good_i3},
    {good_it},
    '{reason_i3}',
    '{reason_it}',
    '{good_tstart}',
    {good_tstart_frac},
    '{good_tstop}',
    {good_tstop_frac},
    '{tstart}',
    {tstart_frac},
    '{tstop}',
    {tstop_frac},
    {nevents},
    {rate},
    {gcd_generated},
    {gcd_bad_doms_validated},
    {gcd_pole_validation},
    {gcd_template_validation},
    {active_strings},
    {active_doms},
    {active_in_ice_doms})
    ON DUPLICATE KEY UPDATE
        good_i3 = {good_i3},
        good_it = {good_it},
        reason_i3 = '{reason_i3}',
        reason_it = '{reason_it}',
        good_tstart = '{good_tstart}',
        good_tstart_frac = {good_tstart_frac},
        good_tstop = '{good_tstop}',
        good_tstop_frac = {good_tstop_frac},
        tstart = '{tstart}',
        tstart_frac = {tstart_frac},
        tstop = '{tstop}',
        tstop_frac = {tstop_frac},
        nevents = {nevents},
        rate = {rate},
        gcd_generated = {gcd_generated},
        gcd_bad_doms_validated = {gcd_bad_doms_validated},
        gcd_pole_validation = {gcd_pole_validation},
        gcd_template_validation = {gcd_template_validation},
        active_strings = {active_strings},
        active_doms = {active_doms},
        active_in_ice_doms = {active_in_ice_doms}
    '''
    
    s = dbs4.fetchall(dbs4_sql, UseDict = True)
    
    counter = 0
    for e in s:
        for k in e.keys():
            if e[k] is None:
                e[k] = 'NULL'
    
        counter += 1

        # Skip 2017 runs:
        if e['run_id'] in [129393, 129394, 129395, 129396, 129397] or e['run_id'] >= 129523:
            print 'Skip 2017 run: {0}'.format(e['run_id'])
            continue
    
        sql = filter_db_sql.format(
            tstart = e['tStart'],
            tstart_frac = e['tStart_frac'],
            tstop = e['tStop'],
            tstop_frac = e['tStop_frac'],
            nevents = e['nEvents'],
            rate = e['rateHz'],
            active_strings = e['ActiveStrings'],
            active_doms = e['ActiveDOMs'],
            active_in_ice_doms = e['ActiveInIceDOMs'],
            gcd_generated = e['GCDCheck'],
            gcd_bad_doms_validated = e['BadDOMsCheck'],
            gcd_pole_validation = e['PoleGCDCheck'],
            gcd_template_validation = e['TemplateGCDCheck'],
            **e
        )
 
        #print sql
   
        print "[{0:>4} / {1}] run_id = {2}".format(counter, len(s), e['run_id'])

        filter_db.execute(sql)

def post_processing(dbs4, filter_db):
    s1 = dbs4.fetchall('SELECT * FROM i3filter.offline_postprocessing', UseDict = True)
    s2 = dbs4.fetchall('SELECT run_id, production_version, validated FROM i3filter.grl_snapshot_info GROUP BY run_id, production_version ORDER BY run_id ASC, production_version ASC', UseDict = True)

    for i in range(len(s2)):
        s2[i]['dataset_id'] = 0
        s2[i]['date_of_validation'] = '2000-01-01 00:00:00'

    s = s1 + s2

    filter_db_sql = '''INSERT INTO `i3filter`.`post_processing`
        (`run_id`,
        `dataset_id`,
        `validated`,
        `date_of_validation`)
        VALUES
        ({run_id},
        {dataset_id},
        {validated},
        '{date_of_validation}')
    ON DUPLICATE KEY UPDATE
        validated = {validated},
        date_of_validation = '{date_of_validation}'
    '''

    counter = 0
    for e in s:
        for k in e.keys():
            if e[k] is None:
                e[k] = 'NULL'
    
        counter += 1

        # Skip 2017 runs and datasets after 1914:
        if e['run_id'] in [129393, 129394, 129395, 129396, 129397] or e['dataset_id'] > 1914 or e['run_id'] >= 129523:
            print 'Skip run because too new: {0}'.format(e['run_id'])
            continue
    
        sql = filter_db_sql.format(**e)

        #print sql

        print "[{0:>4} / {1}] run_id = {2}, dataset_id = {3}".format(counter, len(s), e['run_id'], e['dataset_id'])
        
        filter_db.execute(sql)

def bad_runs(dbs4, filter_db):
    s = dbs4.fetchall("""
SELECT 
    r.run_id, sub_run, status, j.dataset_id
FROM
    i3filter.job j
        JOIN
    i3filter.run r ON r.dataset_id = j.dataset_id
        AND r.queue_id = j.queue_id
        JOIN
    i3filter.grl_snapshot_info g ON r.run_id = g.run_id
WHERE
    status IN ('BadRun' , 'FailedRun')
        AND r.dataset_id IN (1888, 1883, 1874, 1871)
        AND (good_it OR good_i3);
        """, UseDict = True)

    filter_db_sql = """
            INSERT INTO i3filter.sub_runs 
                (run_id, sub_run, bad)
             VALUES ({run_id}, {sub_run_id}, {bad})
             ON DUPLICATE KEY UPDATE bad = {bad}
    """

    counter = 0
    for e in s:
        counter += 1

        # Skip 2017 runs and datasets after 1914:
        if e['run_id'] in [129393, 129394, 129395, 129396, 129397] or e['dataset_id'] > 1914 or e['run_id'] >= 129523:
            print 'Skip run because too new: {0}'.format(e['run_id'])
            continue

        sql = filter_db_sql.format(run_id = e['run_id'], sub_run_id = e['sub_run'], bad = 1)

        #print sql

        print "[{0:>4} / {1}] run_id = {2}/{3}".format(counter, len(s), e['run_id'], e['sub_run'])

        filter_db.execute(sql)

dbs4 = DatabaseConnection.get_connection('dbs4', DummyLogger())
filter_db = DatabaseConnection.get_connection('filter-db', DummyLogger())

print 'Copy run data'
grl_to_runs(dbs4 = dbs4, filter_db = filter_db)
print ''

print 'Copy post processing data'
post_processing(dbs4 = dbs4, filter_db = filter_db)
print ''

print 'Copy bad sub run data'
bad_runs(dbs4 = dbs4, filter_db = filter_db)


