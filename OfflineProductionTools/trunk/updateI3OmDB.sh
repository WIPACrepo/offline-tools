#! /bin/bash

python getDroppedDomsFromLive.py --sql --to-file
scp db_updates.sql IceCube@ppegrid.umons.ac.be:
scp -o 'Host i3db@romaneeconti' -o 'ProxyCommand ssh IceCube@ppegrid.umons.ac.be nc %h %p' updates.sql i3db@romaniconti:
#mysql -u manager I3OmDb < db_updates.sql
#TODAY=`date +%F_%k:%M:%S`
#mv db_updates.sql update_logs/updates_$TODAY.sql
 
