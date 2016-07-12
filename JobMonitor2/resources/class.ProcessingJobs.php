<?php

class ProcessingJobs {
    private $mysql;
    private $live;
    private $result;
    private $dataset_id;
    private $dataset_ids;
    private $default_dataset_id;
    private $l2_dataset_ids;

    private $is_l2_dataset;

    private $dataset_list_only;

    private static $JOB_STATES = array('WAITING','QUEUEING','QUEUED','PROCESSING','OK','ERROR','READYTOCOPY','COPYING','SUSPENDED','RESET','FAILED','COPIED','EVICTED','CLEANING','IDLE','IDLEBDList','IDLEIncompleteFiles','IDLENoFiles','IDLETestRun','IDLEShortRun','IDLELid','IDLENoGCD','BadRun','FailedRun');

    /**
     * @var array Valid states for `status`. The order is important since the first (OK) is not that severe than the last (FAILED).
     */
    private static $RUN_STATUS = array('NONE', 'OK', 'IDLE', 'PROCESSING', 'PROCESSING/ERRORS', 'FAILED');

    public function __construct($host, $user, $password, $db, $default_dataset_id, array $l2_dataset_ids, $api_version, $live_host, $live_user, $live_password, $live_db) {
        $this->mysql = @new mysqli($host, $user, $password, $db);
        $this->live = @new mysqli($live_host, $live_user, $live_password, $live_db);
        $this->result = array('api_version' => $api_version,'error' => 0, 'error_msg' => '', 'error_trace' => '', 'data' => array('runs' => array(), 'datasets' => array(), 'seasons' => array()));
        $this->dataset_id = $default_dataset_id;
        $this->default_dataset_id = $default_dataset_id;
        $this->l2_dataset_ids = $l2_dataset_ids;
        $this->dataset_ids = null;
        $this->dataset_list_only = false;
    }

    private function build_job_status_query($prev) {
        $column = 'status';

        if($prev) {
            $column = 'prev_state';
        }

        $sub_sql = '';
        foreach(self::$JOB_STATES as $status) {
            $sub_sql .= "                        SUM(IF($column = '$status', 1, 0)) AS `jobs_{$column}_{$status}`,\n";
        }

        return $sub_sql;
    }

    private function job_state_decoder($key, $value) {
        $state_column_key_mapping = array('jobs_status' => 'jobs_states', 'jobs_prev_state' => 'jobs_prev_states');
        
        // Get basic key and job status
        $parts = explode('_', $key);
        $status = array_pop($parts);
        $basic = $state_column_key_mapping[implode('_', $parts)];

        return array('category' => $basic, 'status' => $status, 'value' => $value);
    }

    public function execute() {
        $run_pattern = array('run_id' => null, 'sub_runs' => -1, 'date' => null, 'status' => null,
                             'jobs_states' => array(), 'jobs_prev_states' => array(), 'failures' => array(), 'validated' => false,
                             'submitted' => false, 'last_status_change' => '', 'error_message' => array(),
                             'snapshot_id' => null, 'production_version' => null, 'path' => null, 'gcd' => null);

        $this->is_l2_dataset = in_array($this->dataset_id, $this->l2_dataset_ids);

        $this->add_dataset_list();   
        $this->add_season_list();
        $this->validate_datasets();

        if(!$this->dataset_list_only && !$this->result['error']) {
            $this->add_submitted_runs($run_pattern);
            $this->add_paths();
            $this->add_good_run_list($run_pattern);
            $this->validate_runs();
            $this->set_run_states();
            $this->add_error_logs();
        }

        return $this->result;
    }

    private function add_paths() {
        // Add paths to run folder (L2/3)
        $sql = "SELECT run_id,
                    GROUP_CONCAT(DISTINCT path SEPARATOR '*') AS `paths` 
                FROM urlpath u 
                JOIN run r
                    ON r.queue_id = u.queue_id 
                    AND r.dataset_id = u.dataset_id 
                WHERE r.dataset_id = {$this->dataset_id} 
                AND type = 'PERMANENT' 
                GROUP BY run_id";

        $query = $this->mysql->query($sql);
        while($run = $query->fetch_assoc()) {
            $paths = explode('*', $run['paths']);
            $path = $paths[0];

            // If there are more than one path, take the shortest (e.g. if not validated yet,
            // the run has only the folder Run00123456_XX. If validated, it has
            // also the sym link Run00123456. The other path appears in this list
            // since the post processing creates a GapsTar file and inserts it into this
            // table and uses the shorter path.
            if(count($paths) > 0) {
                // Start at '1' since $path has been initialized with index '0'
                for($i = 1; $i < count($paths); ++$i) {
                    if(strlen($path) > strlen($paths[$i])) {
                        $path = $paths[$i];
                    }
                }
            }

            // Since the path starts with a 'file:', remove it
            $path = substr($path, 5);

            // Add it to the list:
            if(isset($this->result['data']['runs'][$run['run_id']])) {
                $this->result['data']['runs'][$run['run_id']]['path'] = $path;
            }
        }

        // Add paths to GCD files
        $sql = "SELECT run_id, name, path 
                FROM urlpath u 
                JOIN run r 
                    ON r.queue_id = u.queue_id 
                    AND r.dataset_id = u.dataset_id 
                WHERE name LIKE '%GCD%' 
                AND r.dataset_id = {$this->dataset_id} 
                AND type = 'INPUT' 
                GROUP BY run_id;";

        $query = $this->mysql->query($sql);
        while($run = $query->fetch_assoc()) {
            // Add it to the list:
            if(isset($this->result['data']['runs'][$run['run_id']])) {
                $this->result['data']['runs'][$run['run_id']]['gcd'] = self::join_paths(substr($run['path'], 5), $run['name']);
            }
        }
    }

    private function validate_datasets() {
        static $validated = false;

        if($validated) {
            return;
        }

        $sql = "SELECT d.dataset_id, ds.season, ds.dataset_type, g.name, ds.comment
                FROM dataset d
                JOIN offline_dataset_season ds
                    ON d.dataset_id = ds.dataset_id
                JOIN offline_working_groups g
                    ON ds.working_group = g.wid
                WHERE ds.enabled";

        $query = $this->mysql->query($sql);
        while($validated = $query->fetch_assoc()) {
            $this->result['data']['datasets'][$validated['dataset_id']]['supported'] = true;
            $this->result['data']['datasets'][$validated['dataset_id']]['season'] = $validated['season'];
            $this->result['data']['datasets'][$validated['dataset_id']]['type'] = $validated['dataset_type'];
            $this->result['data']['datasets'][$validated['dataset_id']]['comment'] = $validated['comment'];

            if($validated['dataset_type'] === 'L3') {
                $this->result['data']['datasets'][$validated['dataset_id']]['working_group'] = $validated['name'];
            }
        }

        $validated = true;
    }

    private function add_season_list() {
        if(count($this->result['data']['seasons'])) {
            return;
        }

        $seasons = array();

        $sql = "SELECT *
                FROM offline_season
                ORDER BY season";

        $query = $this->mysql->query($sql);
        while($season = $query->fetch_assoc()) {
            $season['test_runs'] = strlen($season['test_runs']) === 0 ? array() : explode(',', $season['test_runs']);
            $seasons[$season['season']] = $season;
        }

        $this->result['data']['seasons'] = $seasons;
    }

    private function add_dataset_list() {
        if(!count($this->result['data']['datasets'])) {
            $this->result['data']['datasets'] = $this->get_dataset_ids();
        }
    }

    private function add_error_logs() {
        foreach($this->result['data']['runs'] as $run_id => &$run)  {
            if($run['jobs_states']['ERROR'] + $run['jobs_states']['FAILED'] > 0) {
                $run['error_message'] = $this->get_error_jobs_and_msgs($run_id);
            }
        }
    }

    private function set_run_states() {
        foreach($this->result['data']['runs'] as &$run) {
            if(is_null($run['status'])) {
                $data = &$run['jobs_states'];

                $ok = $data['OK'] + $data['IDLEShortRun'] + $data['IDLENoFiles']
                    + $data['IDLETestRun'] + $data['IDLELid'] + $data['BadRun']
                    + $data['FailedRun'];

                $status = self::get_status('PROCESSING');

                if($ok == $run['sub_runs']) {
                    $status = self::get_status('OK');
                } elseif($ok + $data['FAILED'] == $run['sub_runs']) {
                    $status = self::get_status('FAILED');
                } elseif($data['FAILED'] + $data['ERROR'] > 0) {
                    $status = self::get_status('PROCESSING/ERRORS');
                } elseif($data['IDLE'] > 0) {
                    $status = self::get_status('IDLE');
                }

                $run['status'] = $status;
            }
        }
    }

    private function validate_runs() {
        // Don't use this method for L2 datasets
        if($this->is_l2_dataset) {
            return;
        }

        // If it is a L3 dataset and earlier that season 2015, there is no validation tag
        // So, mark all runs as validated
        if($this->result['data']['datasets'][$this->dataset_id]['season'] < 2015) {
            foreach($this->result['data']['runs'] as &$run) {
                $run['validated'] = true;
            }

            return;
        }

        $sql = "SELECT run_id, validated
                FROM offline_postprocessing
                WHERE dataset_id = {$this->dataset_id}";

        $query = $this->mysql->query($sql);
        while($row = $query->fetch_assoc()) {
            if(isset($this->result['data']['runs'][$row['run_id']])) {
                $this->result['data']['runs'][$row['run_id']]['validated'] = $row['validated'] == 1;
            }
        }
    }

    private function add_good_run_list(array &$run_pattern) {
        // Get first, last, and test runs of dataset
        $season = $this->result['data']['datasets'][$this->dataset_id]['season'];
        $season_info = $this->result['data']['seasons'][$season];

        $first_run_id = $season_info['first_run'];

        // If the first run is <= 0 it is not set yet
        if($first_run_id <= 0) {
            $first_run_id = 99999999;
        }

        // Consider test runs
        // Include test runs of current season
        $season_test_runs = implode(',', $season_info['test_runs']);
        if(count($season_info['test_runs'])) {
            $season_test_runs .= ',';
        }
        
        $next_season_test_runs = '';

        // The last run is either the first run of next season -1 or it has no end
        $last_run_id = 99999999; // No end

        // Check if next season data is available
        if(isset($this->result['data']['seasons'][(string)(intval($season) + 1)])) {
            $next_season = $this->result['data']['seasons'][(string)(intval($season) + 1)];

            // Check if there is already a first run (> 0) for the next season
            if(intval($next_season['first_run']) > 0) {
                $last_run_id = $next_season['first_run'] - 1;
            }

            // Exclude test runs of next season
            $next_season_test_runs = implode(',', $next_season['test_runs']);
            if(count($next_season['test_runs'])) {
                $next_season_test_runs .= ',';
            }
        }

        // Store GRL run numbers:
        // In case there is a processed run that is not in the GRL
        // we need to remove it
        $grl_run_ids = array();

        // Depending on run no. and season, the information is in different tables
        // IceProd1 goes down to run 115975 (season 2010)
        //
        // Required: [run_id, start_date, submitted, snapshot_id, production_version, validated, good_i3, good_it]
        // grl_snapshoot_info: 122205 -> now
        // validateData: 118175 -> 120155 [run_id, live, N/A, live, N/A, validation_status, live, live]
        // pre_processing_checks: 118175 -> 122480

        /*
        +--------+-----------+--------------------------------------------------+
        | season | first_run | test_runs                                        |
        +--------+-----------+--------------------------------------------------+
        |   2010 |    115975 | 115793,115794,115795,115796,115797,115798,115799 |
        |   2011 |    118175 | 118084,118086,118087                             |
        |   2012 |    120156 | 120028,120029,120030                             |
        |   2013 |    122276 | 122205,122206,122207                             |
        |   2014 |    124702 | 124550,124551,124556,124564,124565,124566,124567 |
        |   2015 |    126378 | 126289,126290,126291                             |
        |   2016 |    127950 | 127891,127892,127893                             |
        +--------+-----------+--------------------------------------------------+
        */

        // Holds run information whatever the data source is
        $runs = array();

        if($season >= 2013) {
            // Data source is just grl_snapshot_info

            $sql = "SELECT  run_id,
                            validated,
                            DATE(good_tstart) AS `date`,
                            submitted,
                            MAX(snapshot_id) AS `snapshot_id`,
                            MAX(production_version) AS `production_version`
                    FROM grl_snapshot_info
                    WHERE   (
                                run_id BETWEEN $first_run_id AND $last_run_id OR
                                run_id IN ($season_test_runs -1) /* Have at least -1 to avoid bad SQL */
                            ) AND
                            run_id NOT IN ($next_season_test_runs -1) /* Have at least -1 to avoid bad SQL */ AND
                            (good_it = 1 OR good_i3 = 1)
                    GROUP BY run_id
                    ORDER BY run_id ASC";

            $query = $this->mysql->query($sql);
            while($row = $query->fetch_assoc()) {
                $runs[] = $row;
            }
        } else if($season >= 2010) {
            // Data source is live and validateData
            // validateData has only data for season 2011
            // 2010 and 2013 are getting only data from i3live and any other is default.

            $tmpRuns = array();

            if($season == 2011) {
                $sql = "SELECT  run_id,
                                validation_status AS `validated`
                        FROM validateData
                        WHERE   (
                                    run_id BETWEEN $first_run_id AND $last_run_id OR
                                    run_id IN ($season_test_runs -1) /* Have at least -1 to avoid bad SQL */
                                ) AND
                                run_id NOT IN ($next_season_test_runs -1) /* Have at least -1 to avoid bad SQL */
                        GROUP BY run_id
                        ORDER BY run_id ASC";

                $query = $this->mysql->query($sql);
                while($row = $query->fetch_assoc()) {
                    $tmpRuns[$row['run_id']] = $row;
                }
            }

            // So, now the data from I3Live are still missing
            $sql = "SELECT  runNumber AS `run_id`,
                            DATE(tStart) AS `date`, 
                            MAX(snapshot_id) AS `snapshot_id`
                    FROM livedata_run r
                    JOIN livedata_snapshotrun s 
                        ON s.run_id = r.id 
                    WHERE   (
                                runNumber BETWEEN $first_run_id AND $last_run_id OR
                                runNumber IN ($season_test_runs -1) /* Have at least -1 to avoid bad SQL */
                            ) AND
                            runNumber NOT IN ($next_season_test_runs -1) /* Have at least -1 to avoid bad SQL */ AND
                            (good_it = 1 OR good_i3 = 1)
                    GROUP BY runNumber";

            $query = $this->live->query($sql);
            while($row = $query->fetch_assoc()) {
                if(isset($tmpRuns[$row['run_id']]) && $season == 2011) {
                    $runs[] = array_merge($tmpRuns[$row['run_id']], $row, array('production_version' => -1, 'submitted' => true));
                } else if($season != 2011) {
                    $runs[] = array_merge($row, array('validated' => 1, 'production_version' => -1, 'submitted' => true));
                }
            }
        } else {
            // not supported
            throw new Exception("Season $season is not supported.");
        }

        // Putting all together
        foreach($runs as $run) {
            $this->process_good_run_information($run_pattern, $run['run_id'], $run['validated'], $run['production_version'], $run['snapshot_id'], $run['date']);

            // Store run of GRL
            $grl_run_ids[] = $run['run_id'];
        }

        // Remove all runs from result run list that are not in the GRL
        foreach($this->result['data']['runs'] as $run_id => &$value) {
            if(!in_array($run_id, $grl_run_ids)) {
                unset($this->result['data']['runs'][$run_id]);
            }
        }
    }

    private function process_good_run_information($run_pattern, $run_id, $validated, $production_version, $snapshot_id, $date) {
        if(isset($this->result['data']['runs'][$run_id])) {
            // L2 validation flag is currently stored in grl_snapshot_info
            if($this->is_l2_dataset) {
                $this->result['data']['runs'][$run_id]['validated'] = $validated == 1;
            }
        
            // Add production version and snapshot id
            $this->result['data']['runs'][$run_id]['production_version'] = $production_version;
            $this->result['data']['runs'][$run_id]['snapshot_id'] = $snapshot_id;
        } else {
            $current_run = $run_pattern;
        
            $current_run['run_id'] = $run_id;
            $current_run['date'] = $date;
            $current_run['submitted'] = $this->is_l2_dataset_id && $row['submitted'] == 1;
            $current_run['status'] = self::get_status('NONE');
            $current_run['production_version'] = $production_version;
            $current_run['snapshot_id'] = $snapshot_id;
        
            $this->result['data']['runs'][$run_id] = $current_run;
        }
    }

    private function add_submitted_runs(array &$run_pattern) {
        $sql = "SELECT  r.run_id, 
                        COUNT(sub_run) AS `sub_runs`, 
                        date, 
                        {$this->build_job_status_query(false)}
                        {$this->build_job_status_query(true)}
                        MAX(status_changed) AS `last_status_change`,
                        GROUP_CONCAT(DISTINCT IF(failures > 0, CONCAT_WS('_', sub_run, failures, job_id), '')) AS `failures`
                FROM run r
                JOIN job j
                    ON j.queue_id = r.queue_id
                    AND j.dataset_id = r.dataset_id
                WHERE   r.dataset_id = {$this->dataset_id}
                GROUP BY r.run_id
                ORDER BY r.run_id ASC";

        $query = $this->mysql->query($sql);

        $state_string_start = 'jobs_';

        while($row = $query->fetch_assoc()) {
            $current_run = $run_pattern;

            foreach($row as $key => $value) {
                if(substr($key, 0, strlen($state_string_start)) === $state_string_start) {
                    $info = $this->job_state_decoder($key, $value);
                    $current_run[$info['category']][$info['status']] = $info['value'];
                } elseif($key == 'failures') {
                    // Comes in the pattern ",SUBRUN_FAILURES,SUBRUN_FAILURES" but only for failures > 0
                    $failures = array();

                    $parts = explode(',', $value);
                    foreach($parts as $part) {
                        $info = explode('_', $part);

                        if(count($info) === 3) {
                            $failures[] = array('sub_run' => $info[0], 'failures' => $info[1], 'job_id' => $info[2]);
                        }
                    }

                    $current_run[$key] = $failures;
                } else {
                    $current_run[$key] = $value;
                }
            }

            // This run is obviously submitted
            $current_run['submitted'] = true;

            $this->result['data']['runs'][$row['run_id']] = $current_run;
        }
    }

    public function get_dataset_ids() {
        if(is_null($this->dataset_ids)) {
            $list = array();
            $sql = 'SELECT  dataset_id,
                            description
                    FROM dataset
                    ORDER BY dataset_id DESC';
            $query = $this->mysql->query($sql);
            while($row = $query->fetch_assoc()) {
                $row['selected'] = $row['dataset_id'] == $this->dataset_id;
                $row['supported'] = false;
                $row['season'] = null;
                $row['type'] = null;
                $row['comment'] = '';

                $list[$row['dataset_id']] = $row;
            }
            
            $this->dataset_ids = $list;
        }

        return $this->dataset_ids;
    }

    public function set_dataset_id($dataset_id) {
        $dataset_id = intval($dataset_id);

        $this->add_dataset_list();   
        $this->add_season_list();
        $this->validate_datasets();

        if(isset($this->result['data']['datasets'][(string)$dataset_id]) && $this->result['data']['datasets'][(string)$dataset_id]['supported']) {
            // Set current selection to false
            if($this->dataset_id > 0) {
                $this->result['data']['datasets'][(string)$this->dataset_id]['selected'] = false;
            }

            $this->dataset_id = $dataset_id;

            // Set new selection
            $this->result['data']['datasets'][(string)$dataset_id]['selected'] = true;
        } elseif($dataset_id === -1) {
        } else {
            $this->result['error'] = 1;
            $this->result['error_msg'] = "Dataset $dataset_id doesn't exist or is not supported";
        }
    }

    private function parse_errormessage($msg) {
        $msg = str_replace('<br>', '', $msg);
        $parts =  preg_split('/----([_0-9a-z\.\ ]+)----[\:]{0,1}/i', $msg, -1,  PREG_SPLIT_DELIM_CAPTURE);

        $msgs = array();

        $file = null;
        for($i = 0; $i < count($parts); ++$i) {
            $part = $parts[$i];

            if(0 == $i) {
                $msgs[] = array('file' => '',
                                'content' => $part);
            } elseif($i & 1 == 1) {
                $file = trim($part);
            } else {
                $msgs[] = array('file' => $file,
                                'content' => trim($part));
            }
        }

        return $msgs;
    }

    private function get_error_jobs_and_msgs($run) {
       $sql = " SELECT  submitdir,
                        job_id,
                        status,
                        sub_run,
                        j.queue_id,
                        errormessage 
                FROM    job j
                JOIN    run r 
                        ON j.queue_id = r.queue_id
                WHERE   j.dataset_id = {$this->dataset_id}
                        AND r.dataset_id = {$this->dataset_id}
                        AND (status = 'FAILED' OR status = 'ERROR')
                        AND r.run_id = $run;";

        $query = $this->mysql->query($sql);

        $result = array('ERROR' => array(), 'FAILED' => array());

        while($job = $query->fetch_assoc()) {
            $logs = $this->parse_errormessage($job['errormessage']);

            $result[$job['status']][] = array('job_id' => $job['job_id'],
                                              'sub_run' => $job['sub_run'],
                                              'submitdir' => $job['submitdir'],
                                              'log_tails' => $logs);
        }

        return $result;
    }

    public function set_dataset_list_only($only) {
        $this->dataset_list_only = (bool)$only;
    }

    public static function join_paths() {
        // Found here: http://stackoverflow.com/a/15575293
        $paths = array();
    
        foreach (func_get_args() as $arg) {
            if ($arg !== '') { $paths[] = $arg; }
        }
    
        return preg_replace('#/+#','/',join('/', $paths));
    }

    private static function get_status($name) {
        $ids = array_keys(self::$RUN_STATUS, $name);

        if(count($ids) === 0) {
            throw new InvalidArgumentException("$name does not exists in JOB_STATUS.");
        }

        return array('value' => $ids[0], 'name' => $name);
    }
}
