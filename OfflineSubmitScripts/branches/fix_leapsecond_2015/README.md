
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
+++++ THIS BRANCH IS ONLY FOR LEAPSECOND BUG +++++++++++++++++++++++++++++++
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
+++++ DO NOT MERGE W/ MASTER SINCE IT WILL REMOVE LEAP SECOND CODE +++++++++
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Files which have paths which need to be changed for a new season

* PoleGCDChecks/PoleGCDChecks.py
* TemplateGCDChecks/TemplateGCDChecks.py
* CacheChksums_2015.py
* GCDGenerator_2015.py, function MakeGCD
* SubmitGCDJobs_2015.py
* libs/files.py, just before function get_existing_check_sums()
* crons/* (all files in folder 'crons')

Documentation
-----------------

source in `docs`. Compile with sphinx-apidocs (requires napoleon)
