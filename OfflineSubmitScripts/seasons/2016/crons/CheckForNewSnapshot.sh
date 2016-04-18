#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
$I_ENV python "$DIR/GetRunInfo_2015.py --check"
