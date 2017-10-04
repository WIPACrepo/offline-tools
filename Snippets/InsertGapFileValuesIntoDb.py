
from glob import glob
import re
import pymysql
import os
from icecube.dataclasses import I3Time

exclude_runs = [114899, 113826, 113825]

year = 2016

#files = glob('/data/exp/IceCube/%s/filtered/level2/[0-9][0-9][0-9][0-9]/Run00*[0-9][0-9][0-9]/*_gaps.txt' % year)
#files.extend(glob('/data/exp/IceCube/%s/filtered/level2/[0-9][0-9][0-9][0-9]/*_gaps.txt' % year))
#files.extend(glob('/data/exp/IceCube/%s/filtered/level2a/[0-9][0-9][0-9][0-9]/*_gaps.txt' % year))

#files = glob('/data/exp/IceCube/2011/filtered/level2a/[0-9][0-9][0-9][0-9]/*117315*_gaps.txt')

#files = glob('/data/exp/IceCube/2016/filtered/level2/0930/Run00128565/*_gaps.txt')

files = glob('/data/exp/IceCube/201[0-1]/filtered/level2a/[0-9][0-9][0-9][0-9]/*_gaps.txt')

batch_size = 1000

config = {'sql': True, 'deletion': False}

run_id_pattern = re.compile(r'^/.*Run00([0-9]{6}).*txt$')
sub_run_id_pattern = re.compile(r'^/.*Subrun0+([0-9]+).*txt$')

def GetRunIdFromPath(path, c):
    return int(c.search(path).groups()[0])

def GetSubRunIdFromPath(path, c):
    path = path.replace('Part', 'Subrun')
    return int(c.search(path).groups()[0])

def GetFileLivetime(info):
    start = I3Time(info['first event']['year'], info['first event']['frac'])
    end = I3Time(info['last event']['year'], info['last event']['frac'])

    return (end - start) / 10**9

def ReadGapsFile(path, sub_run_id_pattern, run_id_pattern):
    run_id = GetRunIdFromPath(path, run_id_pattern)

    values = {}

    # Some years have a bug: first gaps file of run has not 'last' event but two 'first' events
    count_first_events = 0

    with open(path, 'r') as file:
        for line in file:
            pair = line.split(':')

            if len(pair) != 2:
                raise

            key = pair[0].strip()
            value = pair[1].strip()

            if pair[0].lower() == 'First Event of File'.lower():
                key = 'first event'

                if count_first_events == 1:
                    # Workaround for double first event bug
                    key = 'last event'

                count_first_events = count_first_events + 1
            elif pair[0].lower() == 'Last Event of File'.lower():
                key = 'last event'

            if pair[0].lower() == 'First Event of File'.lower() or pair[0].lower() == 'Last Event of File'.lower():
                tmp = value.split(' ')
                # Before season 2011, the first event contains the run number
                if (run_id < 118175 and run_id not in [118084, 118086, 118087]) and pair[0].lower() == 'First Event of File'.lower():
                    values['Run'] = int(tmp[0].strip())
                    value = {'event': int(tmp[1].strip()),
                            'year': int(tmp[2].strip()),
                            'frac': int(tmp[3].strip())}
                else:
                    value = {'event': int(tmp[0].strip()),
                            'year': int(tmp[1].strip()),
                            'frac': int(tmp[2].strip())}

            if pair[0].lower() == 'Gap Detected'.lower():
                tmp = value.split(' ')
                key = 'gap'
                
                if run_id < 118175 and run_id not in [118084, 118086, 118087]:
                    tmp[4] = '0'

                value = {'dt': float(tmp[0].strip()),
                        'prev_event_id': int(tmp[1].strip()),
                        'prev_event_frac': int(tmp[2].strip()),
                        'curr_event_id': int(tmp[3].strip()),
                        'curr_event_frac': int(tmp[4].strip())}

                if key not in values.keys():
                    values[key] = []

            if key == 'gap':
                values[key].append(value)
            else:
                values[key] = value

    values['subrun'] = GetSubRunIdFromPath(path, sub_run_id_pattern)

    return values

connection = pymysql.connect(user = 'i3filter', password = '0a6f869d0c8fcc', host = 'filter-db.icecube.wisc.edu', database = 'i3filter')


asql = []

fsql = None

try:
    c = 1

    sql = 'INSERT IGNORE INTO sub_runs (run_id, sub_run, first_event, last_event, first_event_year, first_event_frac, last_event_year, last_event_frac, livetime) VALUES '
    set = '(%s, %s, %s, %s, %s, %s, %s, %s, %s)'

    with connection.cursor() as cursor:
        for file in files:
            print "%s/%s\t%s" % (c, len(files), file)
            run_id = GetRunIdFromPath(file, run_id_pattern)

            if run_id in exclude_runs:
                print "\tSkip run %s" % run_id
                continue

            info = ReadGapsFile(file, sub_run_id_pattern, run_id_pattern)

            if 'Run' not in info.keys():
                info['Run'] = run_id

            if 'File Livetime' not in info.keys():
                info['File Livetime'] = GetFileLivetime(info)

            asql.append(set % (info['Run'], info['subrun'], info['first event']['event'], info['last event']['event'], info['first event']['year'], info['first event']['frac'], info['last event']['year'], info['last event']['frac'], info['File Livetime']))

            if config['sql'] and (c % batch_size == 0 or file == files[-1]):
                print "\t\tWrite %s sets into db" % batch_size
                fsql = '%s %s' % (sql, ','.join(asql))
                cursor.execute('%s %s' % (sql, ','.join(asql)))
                connection.commit()
#                print '%s %s' % (sql, ','.join(asql))

                asql = []

            if config['sql'] and 'gap' in info.keys():
                for gap in info['gap']:
                    gsql = 'INSERT IGNORE INTO gaps (run_id, sub_run, prev_event_id, curr_event_id, delta_time, prev_event_frac, curr_event_frac) VALUES (%s, %s, %s, %s, %s, %s, %s)'
                    cursor.execute(gsql, (info['Run'], info['subrun'], gap['prev_event_id'],  gap['curr_event_id'],  gap['dt'],  gap['prev_event_frac'], gap['curr_event_frac']))
                    print "\t\tWrite gap: %s" % (gsql % (info['Run'], info['subrun'], gap['prev_event_id'],  gap['curr_event_id'],  gap['dt'],  gap['prev_event_frac'], gap['curr_event_frac']))
                    connection.commit()

            if config['deletion']:
                print '\t\tgaps file deleted'
                os.remove(file)

            c = c + 1
except pymysql.err.ProgrammingError as e:
    print asql
    print fsql
    raise e
except pymysql.err.IntegrityError as e:
    print asql
    print fsql
    raise e
finally:
    connection.close()

