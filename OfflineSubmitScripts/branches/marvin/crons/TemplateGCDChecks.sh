#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
$I_ENV python "$DIR/TemplateGCDChecks.py --debug" > "$DIR/logs/TemplateGCDChecks/output.log" 2>&1 
