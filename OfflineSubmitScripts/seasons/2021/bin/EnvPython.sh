#!/bin/sh

unset PYTHONPATH
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

eval `/cvmfs/icecube.opensciencegrid.org/py3-v4.1.0/setup.sh`
#eval `/cvmfs/icecube.opensciencegrid.org/iceprod/v2.5.3/setup.sh`

# Load config in order to get current L2 icerec path
eval "$(python3 ${DIR}/bin/ConfigLoader.py)"

echo "I3_BUILD=${Level2[i3_build]}"

"${Level2[i3_build]}/./env-shell.sh" python3 $@
