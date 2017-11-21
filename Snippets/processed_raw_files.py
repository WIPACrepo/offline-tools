
import sys
import os
from datetime import datetime

sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2017/')
from libs.databaseconnection import DatabaseConnection
from libs.logger import DummyLogger
from libs.path import remove_path_prefix

def is_ic79(run_id):
    test_runs = (115933, 115931, 115930, 115927, 115925, 115883, 115882, 115881, 115848, 115847, 115846, 115845, 115844, 115839, 115838, 115837, 115836, 115835, 115799, 115798, 115797, 115796, 115794, 115793, 115728, 115727, 115725, 115723, 115600, 115599, 115579, 115488)
    ntext_runs = (118084, 118086, 118087)
    return (115969 <= run_id < 118175 or run_id in test_runs) and run_id not in ntext_runs

db = DatabaseConnection.get_connection('dbs4', DummyLogger())

data = db.fetchall("""
SELECT 
    r.run_id, u.path, u.name
FROM
    i3filter.job j
        JOIN
    i3filter.urlpath u ON u.dataset_id = j.dataset_id
        AND u.queue_id = j.queue_id
        JOIN
    i3filter.run r ON r.dataset_id = j.dataset_id
        AND r.queue_id = j.queue_id
WHERE
    j.dataset_id IN (1911 , 1928, 1929)
        AND j.status = 'OK'
        AND u.type = 'INPUT'
        AND u.name NOT LIKE '%GCD%'
ORDER BY j.status_changed
""")

ic79_ok = db.fetchall("""SELECT 
    run_id
FROM
    i3filter.grl_snapshot_info_pass2
WHERE
    (run_id BETWEEN 115969 AND 118175 - 1
        OR run_id IN (115933 , 115931,
        115930,
        115927,
        115925,
        115883,
        115882,
        115881,
        115848,
        115847,
        115846,
        115845,
        115844,
        115839,
        115838,
        115837,
        115836,
        115835,
        115799,
        115798,
        115797,
        115796,
        115794,
        115793,
        115728,
        115727,
        115725,
        115723,
        115600,
        115599,
        115579,
        115488))
        AND run_id NOT IN (118084 , 118086, 118087) AND (good_it OR good_i3);""")

ic79_ok = [int(r['run_id']) for r in ic79_ok]

fdb = DatabaseConnection.get_connection('filter-db', DummyLogger())

validated = fdb.fetchall('SELECT * FROM i3filter.post_processing WHERE (dataset_id IN (1911, 1928, 1929)/* OR (dataset_id = 1929 AND run_id BETWEEN 117681 AND 117849)*/) AND validated')
validated = [int(d['run_id']) for d in validated]

with open(os.path.join('/home/joertlin/ForGonzalo/PFRawProcessing', 'processed_files_{0:%Y-%m-%d_%H-%M-%S}.txt'.format(datetime.now())), 'w') as f:
    for row in data:
        if int(row['run_id']) in validated:
            if is_ic79(int(row['run_id'])):
                if int(row['run_id']) not in ic79_ok:
                    continue

            f.write(os.path.join(remove_path_prefix(row['path']), row['name']) + '\n')

