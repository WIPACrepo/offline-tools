#!/bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

eval `/cvmfs/icecube.opensciencegrid.org/py2-v2/setup.sh`

# Load config in order to get current L2 icerec path
eval "$(python ${DIR}/crons/ConfigLoader.py)"

"${L2[i3_build]}/./env-shell.sh" python $@
