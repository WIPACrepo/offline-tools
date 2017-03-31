
import sys

sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2016/')
from libs.databaseconnection import DatabaseConnection
from libs.logger import DummyLogger


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
'''

dbs4 = DatabaseConnection.get_connection('dbs4', DummyLogger())
filter_db = DatabaseConnection.get_connection('filter-db', DummyLogger())

s = dbs4.fetchall(dbs4_sql, UseDict = True)

counter = 0
for e in s:
    for k in e.keys():
        if e[k] is None:
            e[k] = 'NULL'

    counter += 1

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

    print "[{0:>4} / {1}] run_id = {2}".format(counter, len(s), e['run_id'])

    filter_db.execute(sql)
