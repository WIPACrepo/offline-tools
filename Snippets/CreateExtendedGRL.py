
import os
import re
import sys

import argparse

from CompareGRLs import read_file
from glob import glob

sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2016/')
sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineProductionTools')
from libs.databaseconnection import DatabaseConnection
from libs.logger import DummyLogger
from RunTools import RunTools

class GoodRunListGenerator:
    def __init__(self, skip):
        self.active_doms = None
        self.first_run = None
        self.last_run = None
        self.skip = skip

    def get_gcd_file(self, path, run_id):
        files = glob(os.path.join(path, "Level2*%s*GCD*.i3.gz" % run_id))
    
        if len(files) != 1:
            raise Exception('Did not find exactly one GCD file in %s: %s' % (path, files))
    
        return files[0]
    
    def get_data_files(self, path, run_id):
      # Do NOT use just a * because the you also get the IT, EHE, SLOP etc files.
      g = os.path.join(path, 'Level2*%s*Subrun00000[0-9][0-9][0-9].i3.bz2' % run_id)
      files = glob(g)
    
      if not files:
        files.extend(glob(os.path.join(path, 'Level2*%s*Part00000[0-9][0-9][0-9].i3.bz2' % run_id)))
    
      return files
    
    def get_subrun_number(self, path):
        c = re.compile(r'^/.*0{5}0*([0-9]+).*i3\.bz2$')
        return int(c.search(path).groups()[0])
    
    def get_sub_run_lifetime(self, run_id, sub_run_id, logger, pass_number):
        run_id = int(run_id)
        result = { 
            'run_id': run_id,
            'sub_run': sub_run_id,
            'livetime': None,
            'gaps': None,
            'gaps_time': None
        }   
    
        pass_x_folder_str = ''
        if pass_number > 1:
            pass_x_folder_str = '_pass2'
    
        # Databases
        filter_db = DatabaseConnection.get_connection('filter-db', logger)
    
        # Load livetime from gaps files/sub_run table
        sql = "SELECT SUM(livetime) AS `livetime` FROM i3filter.sub_runs%s WHERE run_id = %s AND sub_run = %s" % (pass_x_folder_str, run_id, sub_run_id)
        data = filter_db.fetchall(sql, UseDict = True)[0]
        result['livetime'] = data['livetime']
    
        # Load total gaps lifetime
        sql = "SELECT COUNT(*) AS `gaps`, SUM(delta_time) AS `livetime` FROM i3filter.gaps%s WHERE run_id = %s AND sub_run = %s" % (pass_x_folder_str, run_id, sub_run_id)
        data = filter_db.fetchall(sql, UseDict = True)[0]
        result['gaps'] = data['gaps']
        result['gaps_time'] = data['livetime']
    
        if result['gaps_time'] is None:
            result['gaps_time'] = 0 
       
        result['livetime'] = result['livetime'] - result['gaps_time']
    
        return result
    
    def create_list(self, season, pass_number):
        pass_x_folder_str = ''
        if pass_number > 1:
            pass_x_folder_str = 'pass2'
  
        path_column = 7
 
        if pass_number == 2:
            path_column = 4
            path = "/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_pass2/tmp/GRL_%s.txt" % (season)
        else: 
            path = "/data/exp/IceCube/%s/filtered/level2%s/IC86_%s_GoodRunInfo.txt" % (season, pass_x_folder_str, season)
    
        grl = read_file(path)
   
        if pass_number == 2:
            for run_id in grl.keys():
                grl[run_id][path_column] = sorted(glob("%s_*" % grl[run_id][path_column].rstrip('/')))[-1]
 
        sorted_runs = sorted(grl.keys())
   
        self.first_run = sorted_runs[0]
        self.last_run = sorted_runs[-1]
 
        data = [['# Run ID', 'Sub Run ID', 'Livetime', 'ActiveStrings', 'ActiveDoms', 'ActiveInIce', 'GCD File', 'File path']]
    
        print "Process %s runs" % len(sorted_runs)
    
        counter = 0
    
        for run_id in sorted_runs:
            counter += 1
    
            print "Process run %s [%s / %s]" % (run_id, counter, len(sorted_runs))
   
            if int(run_id) in self.skip:
                print "  skip this run"
                continue
 
            files = self.get_data_files(grl[run_id][path_column], run_id)
            gcd_file = self.get_gcd_file(grl[run_id][path_column], run_id)
    
            active_strings, active_doms, active_in_ice = self.get_active_dom_data(run_id, pass_number, gcd_file)
    
            files.sort()
            for f in files:
                if os.path.getsize(f) < 100000:
                    continue

                sub_run = self.get_subrun_number(f)
                filepath = f
    
                livetime = self.get_sub_run_lifetime(run_id, sub_run, DummyLogger(), pass_number)['livetime']
    
                data.append([run_id, sub_run, livetime, active_strings, active_doms, active_in_ice, gcd_file, filepath])
 
        return data
    
    def get_active_dom_data(self, run_id, pass_number, gcd_file):
        if self.active_doms is None:
            print "Load Active* Data from grl_snapshot_info"

            pass_str = ''
            if pass_number > 1:
                pass_str = '_pass2'

            sql = "SELECT * FROM i3filter.grl_snapshot_info%s WHERE run_id BETWEEN %s AND %s ORDER BY run_id, snapshot_id" % (pass_str, self.first_run, self.last_run)
            db = DatabaseConnection.get_connection('dbs4', DummyLogger())

            data = db.fetchall(sql, UseDict = True)
            self.active_doms = {}

            for row in data:
                self.active_doms[str(row['run_id'])] = row

        if run_id in self.active_doms:
            if self.active_doms[run_id]['ActiveStrings'] is not None and self.active_doms[run_id]['ActiveDOMs'] is not None and self.active_doms[run_id]['ActiveInIceDOMs'] is not None:
                return (self.active_doms[run_id]['ActiveStrings'], self.active_doms[run_id]['ActiveDOMs'], self.active_doms[run_id]['ActiveInIceDOMs'])

        print "  checking GCD file for active DOMs..."
        run_tools = RunTools(run_id, passNumber = pass_number)
        return run_tools.GetActiveStringsAndDoms(Season = None, UpdateDB = False, gcd_file = gcd_file)
    
    
    def get_max_with_of_column(self, data, column):
        m = 0
    
        for row in data:
            if len(str(row[column])) > m:
                m = len(str(row[column]))
    
        return m
    
    def create_file(self, data, filename):
        widths = [self.get_max_with_of_column(data, c) for c in range(len(data[0]))]
    
        with open(filename, 'w') as f:
            first = True
    
            for row in data:
                if first:
                    first = False
                    line = [str(row[c]).ljust(widths[c]) for c in range(len(row))]
                else:
                    line = [str(row[c]).rjust(widths[c]) for c in range(len(row))]
    
                f.write('   '.join(line) + '\n')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', help = "output file path", type = str, required = True)
    parser.add_argument('--season', help = "season, e.g. 2011, 2012, 2013.", type = int, required = True)
    parser.add_argument('--pass-number', help = "Pass1 = 1 (default), pass2 = 2, etc.", required = False, default = 1, type = int)
    args = parser.parse_args()

    grl_generator = GoodRunListGenerator(skip = [122202])
    grl_generator.create_file(grl_generator.create_list(args.season, args.pass_number), args.out)

