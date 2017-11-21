
from icecube import dataclasses, icetray, dataio
import argparse
import os
import re
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument('--file', required = True, help = "The i3 file", type = str)
parser.add_argument('--output-folder', required = True, help = "Path to folder where the output file should be stored", type = str)
args = parser.parse_args()

def modify_meta_xml(src, dest, data):
    def replace_tag(text, tag, replacement):
        return re.sub(r'\<{tag}\>(.+)\<\/{tag}\>'.format(tag = tag), '<{tag}>{repl}</{tag}>'.format(tag = tag, repl = replacement), text, 0, re.MULTILINE)

    tag = {
        'DIF_Creation_Date': datetime.now().strftime('%Y-%m-%d'),
        'Run_Number': data['run_id'],
        'First_Event': data['first_frame']['event_id'],
        'Last_Event': data['last_frame']['event_id'],
        'Start_DateTime': data['first_frame']['start_time'],
        'End_DateTime': data['last_frame']['end_time'],
        'Summary': '\g<1>\nThis data is recovered data from PFRaw.',
        'Entry_ID': os.path.splitext(os.path.splitext(os.path.basename(data['i3file']))[0])[0]
    }

    srccontent = None

    with open(src) as srcf:
        srccontent = srcf.read()

    destcontent = srccontent
    for tag, repl in tag.items():
        destcontent = replace_tag(destcontent, tag, repl)

    with open(dest, 'w') as destf:
        destf.write(destcontent)

outfile = os.path.join(args.output_folder, os.path.basename(os.path.splitext(os.path.splitext(args.file)[0])[0] + '.meta.xml'))

print 'Write file: {}'.format(outfile)

fcounter = 0
header = None
first_header = None

f = dataio.I3File(args.file)

while f.more():
    frame = f.pop_frame()

    if 'I3EventHeader' in frame:
        header = frame['I3EventHeader']

        if first_header is None:
            first_header = frame['I3EventHeader']

    fcounter += 1

print '{} frames read'.format(fcounter)

metadata_out = {
    'i3file': args.file,
    'date': str(datetime.now()),
    'run_id': str(int(args.file.split('Run00')[1].split('_')[0])),
    'last_frame': {
        'start_time': header.start_time.date_time.strftime('%Y-%m-%dT%H:%M:%S'),
        'end_time': header.end_time.date_time.strftime('%Y-%m-%dT%H:%M:%S'),
        'event_id': header.event_id,
        'run_id': header.run_id
    },
    'first_frame': {
        'start_time': first_header.start_time.date_time.strftime('%Y-%m-%dT%H:%M:%S'),
        'end_time': first_header.end_time.date_time.strftime('%Y-%m-%dT%H:%M:%S'),
        'event_id': first_header.event_id,
        'run_id': first_header.run_id
    }   
}

modify_meta_xml('/home/joertlin/workspace/Sandbox/Snippets/data/PFDST_meta_template.xml', outfile, metadata_out)
