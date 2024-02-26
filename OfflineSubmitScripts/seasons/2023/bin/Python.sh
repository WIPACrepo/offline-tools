#!/bin/sh
unset PYTHONPATH
eval `/cvmfs/icecube.opensciencegrid.org/py3-v4.3.0/setup.sh`
python $@
