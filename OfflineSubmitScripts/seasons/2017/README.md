
Configuration
-----------------

The configuration is stored at two places:

    1. `config/offline_processing.cfg.`
    2. MySQL DB: Host: filter-db, DB: i3filter, Tables: datasets, seasons, working_groups, level2_paths, level3_outdir

The database (2) only contains configurations about the seasons (test runs and run starts), datasets (IDs, L1, L2, L3, etc) and the working groups as the table names already say.
All other configuration is done in the config file (1). This includes ALL paths, etc. You should never need to touch a python file in orde rto reconfigure anything.

Important cofiguration:

    1. DEFAULT: Season: Specifies the current season.
    2. Level2: I3_SRC: Source folder of the icerec release
    3. Level2: I3_BUILD: Build folder of the icerec release

Execute Scripts
-----------------

The `bin` folder contains the bash script `EnvPython.sh`. This is a shortcut for `path/to/build/./env-shell.sh python`. The path to the icerec build is read from the config file (1). Since all scripts require the icerec environment, use the `EnvPython.sh` rather than `python`.

Example: `./bin/EnvPython.sh MainSubmit_L2.py --dataset-id 1234 --runs 123456 123457 1234568`


Cron Jobs
-----------------

Several installed cron jobs are helping out. All of them are installed on cobalt08 (user: i3filter).

    1. `CacheChecksums.py`: Calculates the MD5 checksums of PFFilt files. The sums are required for run submission and the cache makes it way faster to submit runs.
    2. `GetRunInfo.py --check`: Checks if new run data is available. If so, an email will be sent
    3. `PostProcessing_L2.py --cron`: Executes the post processing automatically.


Documentation
-----------------

Source in `docs`. Compile with sphinx-apidocs (requires napoleon)
