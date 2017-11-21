
from icecube import dataclasses, icetray, dataio
import argparse
import os
import re
from datetime import datetime
import json

parser = argparse.ArgumentParser()
parser.add_argument('--filename', required = True, help = "The filename (w/o path)", type = str)
parser.add_argument('--folders', nargs = '+', required = True, help = "The path to the file. If this file has several copies, add several paths. The largest file will be chosen.", type = str)
parser.add_argument('--output-folder', required = True, help = "Path to folder where the output file should be stored", type = str)
args = parser.parse_args()

def get_output(fn):
    return 'recovered-data_' + os.path.basename(fn)

#print 'Input file:   {}'.format(args.filename)
#print 'Folders file: {}'.format(args.folders)

# Chose file
files = [os.path.join(f, args.filename) for f in args.folders]

max_size = -1
infile = None
for f in files:
    s = os.path.getsize(f)

    if s > max_size:
        max_size = s
        infile = f

# Output file
out = os.path.join(args.output_folder, get_output(infile))

# Infos
print 'Infile:  {}'.format(infile)
print 'Outfile: {}'.format(out)

def modify_meta_xml(src, dest, data):
    def replace_tag(text, tag, replacement):
        return re.sub(r'\<{tag}\>(.+)\<\/{tag}\>'.format(tag = tag), '<{tag}>{repl}</{tag}>'.format(tag = tag, repl = replacement), text, 0, re.MULTILINE)

    tag = {
        'DIF_Creation_Date': datetime.now().strftime('%Y-%m-%d'),
        'First_Event': data['first_frame']['event_id'],
        'Last_Event': data['last_frame']['event_id'],
        'Start_DateTime': data['first_frame']['start_time'],
        'End_DateTime': data['last_frame']['end_time'],
        'Summary': '\g<1>\nThis data is recovered data from a corrupted file at the South Pole.\n{}s out of {}s of data has been recovered.'.format(int(round(data['recovered']['time'])), int(round(data['metadata']['duration'])))
    }

    srccontent = None

    with open(src) as srcf:
        srccontent = srcf.read()

    destcontent = srccontent
    for tag, repl in tag.items():
        destcontent = replace_tag(destcontent, tag, repl)

    with open(dest, 'w') as destf:
        destf.write(destcontent)

def get_start_stop_from_xml(infile):
    f = os.path.splitext(infile)[0] + '.meta.xml'

    print 'Metadata: {}'.format(f)

    if not os.path.isfile(f):
        raise RuntimeError('Metadata file does not exist')

    rstart = r'\<Start_DateTime\>(\d{4}\-\d{2}\-\d{2}T\d{2}\:\d{2}\:\d{2})\<\/Start_DateTime\>'
    rend = r'\<End_DateTime\>(\d{4}\-\d{2}\-\d{2}T\d{2}\:\d{2}\:\d{2})\<\/End_DateTime\>'

    cstart = re.compile(rstart)
    cend = re.compile(rend)

    tstart = None
    tend = None

    with open(f) as mf:
        for line in mf:
            s = cstart.search(line)
            if s is not None:
                tstart = s.groups()[0]
            else:
                e = cend.search(line)
                if e is not None:
                    tend = e.groups()[0]

    return datetime.strptime(tstart, '%Y-%m-%dT%H:%M:%S'), datetime.strptime(tend, '%Y-%m-%dT%H:%M:%S')

def write_metadata_of_rec(f, data):
    with open(f + '.json', 'w') as fh:
        json.dump(data, fh)

def recover_frames(infile, outfile):
    metadata = get_start_stop_from_xml(infile)
    metadata_duration = (metadata[1] - metadata[0]).total_seconds()

    print 'Metadata start:  {}'.format(metadata[0])
    print 'Metadata end:    {}'.format(metadata[1])
    print 'Metadata length: {}s'.format(metadata_duration)

    f = dataio.I3File(infile)
    of = dataio.I3File(outfile, 'w')

    fcounter = 0
    header = None
    first_header = None

    try:
        while f.more():
            frame = f.pop_frame()

            if 'I3EventHeader' in frame:
                header = frame['I3EventHeader']

                if first_header is None:
                    first_header = frame['I3EventHeader']

            of.push(frame)
            fcounter += 1
    except Exception as e:
        print 'Exception {} raised. Stop recovering data...'.format(e)
    finally:
        f.close()
        of.close()

        print 'Read {} frames'.format(fcounter)
        print 'Last event header: {}'.format(header)

        recovered = (header.end_time.date_time - first_header.start_time.date_time).total_seconds()
        lost = metadata_duration - recovered

        print 'Lost data in seconds: {}'.format(lost)
        print 'Recovered data in seconds: {}'.format(recovered)

        metadata_out = {
            'infile': infile,
            'outfile': outfile,
            'date': str(datetime.now()),
            'metadata': {
                'start': metadata[0].strftime('%Y-%m-%dT%H:%M:%S'),
                'end': metadata[1].strftime('%Y-%m-%dT%H:%M:%S'),
                'duration': metadata_duration,
                'duration_first_event_header': (metadata[1] - first_header.end_time.date_time).total_seconds()
            },
            'lost': lost,
            'recovered': {
                'time': recovered,
                'frames': fcounter
            },
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

        write_metadata_of_rec(outfile, metadata_out)

        metain = os.path.splitext(infile)[0] + '.meta.xml'
        metaout = os.path.splitext(outfile)[0] + '.meta.xml'
        modify_meta_xml(metain, metaout, metadata_out)

recover_frames(infile, out)

print '-------------------------------------------------------------------------------------------------'
print '-------------------------------------------------------------------------------------------------'

