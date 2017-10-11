
import sys
sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2017/')
from libs.logger import DummyLogger
from libs.databaseconnection import DatabaseConnection
from datetime import datetime

sources = [
    {'file': '/home/joertlin/tmp/pass2-lost-files/lists-171009/missing.pfraw.ic79.names', 'comment': '', 'times': '/home/joertlin/tmp/pass2-lost-files/lists-171009/missing.pfraw.ic79.times'},
    {'file': '/home/joertlin/tmp/pass2-lost-files/lists-171009/good.not.on.tape', 'comment': 'Lost for sure', 'times': '/home/joertlin/tmp/pass2-lost-files/lists-171009/good.not.on.tape.times'},
    {'file': '/home/joertlin/tmp/pass2-lost-files/lists-171009/missing.pfdst.ic86.ontape.names', 'comment': 'Tiny chance to recover it', 'times': '/home/joertlin/tmp/pass2-lost-files/lists-171009/missing.pfdst.ic86.ontape.times'},
]

def get_start_time_of_run(run_id, db):
    query = db.fetchall('SELECT tstart FROM i3filter.runs WHERE run_id = {} ORDER BY snapshot_id DESC LIMIT 1'.format(run_id), UseDict = True)

    return query[0]['tstart']

def file_reader(path, db):
    data = []

    times = []

    with open(path['times'], 'r') as f:
        for row in f:
            columns = row.split()

            start = datetime.strptime(' '.join(columns[0:2]), '%Y-%m-%d %H:%M:%S')
            stop = datetime.strptime(' '.join(columns[2:4]), '%Y-%m-%d %H:%M:%S')

            times.append({'start': start, 'stop': stop})

    with open(path['file'], 'r') as f:
        for line, row in enumerate(f):
            columns = row.split()

            filename = columns[-1]
            run_id = int(filename.split('Run00')[1].split('_Subrun00000000_00')[0])
            sub_run_id = int(filename.split('Run00')[1].split('_Subrun00000000_00')[1].split('.')[0])
            tstart = get_start_time_of_run(run_id, db)

            fpath = '/data/exp/IceCube/{year}/unbiased/PFRaw/{month:0>2}{day:0>2}/{name}'.format(year = tstart.year, month = tstart.month, day = tstart.day, name = filename)

            ftype = filename.split('_')[0][2:] if not filename.startswith('key') else filename.split('_')[2][2:]

            # bugfix
            if run_id == 125790 and sub_run_id in (89, 113):
                times[line]['start'] = times[line]['start'].replace(year = 2015)

            livetime = (times[line]['stop'] - times[line]['start']).total_seconds()

            data.append({
                'path': fpath,
                'run_id': run_id,
                'sub_run': sub_run_id,
                'tstart': tstart,
                'type': ftype,
                'comment': path['comment'],
                'start': times[line]['start'],
                'stop': times[line]['stop'],
                'livetime': livetime if livetime > 0 else 'NULL'
            })

            if (abs(livetime) < 1 and times[line]['stop'] != times[line]['start']) or livetime > 500:
                print 'Run/subrun: {0}/{1}'.format(run_id, sub_run_id)
                print 'livetime:   {}'.format(livetime)
                print 'start:      {}'.format(times[line]['start'])
                print 'stop:       {}'.format(times[line]['stop'])

            if livetime == 0:
                print 'No livetime available for {0}/{1}'.format(run_id, sub_run_id)

            if run_id < 100000:
                print data[-1]
                raise RuntimeError('Invalid run id')

            if sub_run_id < 0 or sub_run_id > 500:
                print data[-1]
                raise RuntimeError('Invalid sub run id')

            if data[-1]['type'] not in ('Raw', 'SDST', 'Filt'):
                print data[-1]
                raise RuntimeError('Unknown file type')

    return data

db = DatabaseConnection.get_connection('filter-db', DummyLogger())

data = []

for sfile in sources:
    data.extend(file_reader(sfile, db))

print 'Found {} missing files'.format(len(data))

print 'Write to DB'

no_lt = 0
counter = 0
for entry in data:
    if entry['type'] not in ('Raw', 'SDST'):
        #print 'Skip entry {}'.format(entry)
        continue

    if entry['livetime'] == 'NULL':
        no_lt += 1

    sql = 'INSERT INTO i3filter.missing_files_pass2 (`run_id`, `sub_run`, `type`, `path`, `comment`, `last_change`, `livetime`) VALUES ({run_id}, {sub_run}, \'{type}\', \'{path}\', \'{comment}\', NOW(), {livetime}) ON DUPLICATE KEY UPDATE `path` = VALUES(`path`), `comment` = VALUES(`comment`), `last_change` = NOW(), `livetime` = VALUES(`livetime`)'
    sql = sql.format(**entry)

    try:
        db.execute(sql)
    except Exception as e:
        print entry
        print sql
        raise e
    else:
        counter += 1
        print '{0} / {1}\tEntered missing file'.format(counter, len(data))

print 'No lietime available for {} files'.format(no_lt)

print entry
