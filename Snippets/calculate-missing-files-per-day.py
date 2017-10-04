
import os
from datetime import datetime
from glob import glob

def missing_files(filename):
    data = {}

    aff_runs = set()

    missing_files_per_run = {}

    with open(filename, 'r') as f:
        for line in f:
            parts = line.split()

            date = parts[3]

            if date in data:
                data[date] += 1
            else:
                data[date] = 1

            run_id = int(parts[2].split('Run00')[1].split('_Subrun')[0])

            if run_id not in missing_files_per_run:
                missing_files_per_run[run_id] = 0

            missing_files_per_run[run_id] += 1

            aff_runs.add(run_id)

    return data, aff_runs, missing_files_per_run

def find_available_files(date):
    date = datetime.strptime(date, '%Y-%m-%d')
    path = '/data/exp/IceCube/{date:%Y}/unbiased/PFDST/{date:%m%d}/*.tar.gz'.format(date = date)
    return len(glob(path))

data, aff_runs, missing_files_per_run = missing_files('/home/joertlin/tmp/missing.files.without.info')
detailed_data = {date: {'missing': missing, 'available': find_available_files(date)} for date, missing in data.items()}

sorted_keys = sorted(detailed_data.keys())

print 'Date\t\tMissing\tAvailable\tMissing in %'
print '-----------------------'

for date in sorted_keys:
    data = detailed_data[date]

    print '{date}\t{missing}\t{available}\t\t{missing_p:.2%}'.format(date = date, missing = data['missing'], available = data['available'], missing_p = float(data['missing']) / (data['missing'] + data['available']))

print '-----------------------'
print 'Total missing: {}'.format(sum([value['missing'] for date, value in detailed_data.items()]))
print ''
print 'Affected runs: {}'.format(len(aff_runs))
for r in aff_runs:
    print r
print ''
print 'Missing files per run:'
for run_id, n in missing_files_per_run.items():
    print '{0}: {1}'.format(run_id, n)


