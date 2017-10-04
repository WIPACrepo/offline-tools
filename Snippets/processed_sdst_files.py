
import sys
import os
from datetime import datetime

sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2017/')
from libs.databaseconnection import DatabaseConnection
from libs.logger import DummyLogger
from libs.path import remove_path_prefix

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
    j.dataset_id BETWEEN 1916 AND 1922
        AND j.status = 'OK'
        AND u.type = 'INPUT'
        AND u.name NOT LIKE '%GCD%'
        AND u.name LIKE 'PFDST%'
ORDER BY j.status_changed
""")

fdb = DatabaseConnection.get_connection('filter-db', DummyLogger())

validated = fdb.fetchall('SELECT * FROM i3filter.post_processing WHERE dataset_id BETWEEN 1916 AND 1922 AND dataset_id NOT IN (1920, 1921) AND validated')
validated = [int(d['run_id']) for d in validated]

with open(os.path.join('/home/joertlin/ForGonzalo/PFDSTProcessing', 'processed_files_{0:%Y-%m-%d_%H-%M-%S}.txt'.format(datetime.now())), 'w') as f:
    for row in data:
        if int(row['run_id']) in validated:
            f.write(os.path.join(remove_path_prefix(row['path']), row['name']) + '\n')
