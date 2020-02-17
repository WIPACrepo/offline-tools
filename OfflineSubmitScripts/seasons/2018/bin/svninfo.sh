#!/bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

eval `/cvmfs/icecube.opensciencegrid.org/py2-v2/setup.sh`

# Load config in order to get current L2 icerec path
eval "$(python ${DIR}/bin/ConfigLoader.py)"

cd $DIR

"${Level2[i3_build]}/./env-shell.sh" python -c "import json; import os; from libs.path import get_rootdir, get_tmpdir; import libs.svn; from libs.logger import DummyLogger; logger = DummyLogger(); logger.silence = True; svn = libs.svn.SVN(get_rootdir(), logger); f = open(os.path.join(get_tmpdir(), 'svninfo.txt'), 'w'); svn.get('URL'); f.write(json.dumps(svn._data)); f.close()"


