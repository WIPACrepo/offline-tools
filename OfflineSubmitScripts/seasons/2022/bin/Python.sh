#!/bin/sh
unset PYTHONPATH
eval `/cvmfs/icecube.opensciencegrid.org/py3-v4.1.0/setup.sh`
python $@
