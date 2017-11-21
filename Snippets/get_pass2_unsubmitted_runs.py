
import argparse
import requests

parser = argparse.ArgumentParser()
parser.add_argument('dataset_id', help = "dataset id", type = int)
args = parser.parse_args()

r = requests.get('https://grid.icecube.wisc.edu/filtering/CalendarView/query.php?dataset_id={}'.format(args.dataset_id))
data = r.json()
not_submitted = [run_id for run_id, d in data['data']['runs'].items() if not d['submitted'] and (d['good_it'] or d['good_i3'])]
print 'Not submitted yet: {}'.format(len(not_submitted))
print ' '.join(not_submitted)
