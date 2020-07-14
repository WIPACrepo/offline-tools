<?php

require_once('class.Rest.php');

class ProcessingJobs {
    private $mysql;
    private $live;
    private $result;
    private $dataset_id;
    private $iceprod_id;
    private $dataset_ids;
    private $default_dataset_id;
    private $pass2;
    private $rest;

    private $dataset_list_only;

    private static $JOB_STATES = array('WAITING','QUEUEING','QUEUED','PROCESSING','OK','ERROR','READYTOCOPY','COPYING','SUSPENDED','RESET','FAILED','COPIED','EVICTED','CLEANING','IDLE','IDLEBDList','IDLEIncompleteFiles','IDLENoFiles','IDLETestRun','IDLEShortRun','IDLELid','IDLENoGCD','BadRun','FailedRun');

    /**
     * @var array Valid states for `status`. The order is important since the first (OK) is not that severe than the last (FAILED).
     */
    private static $RUN_STATUS = array('NONE', 'OK', 'IDLE', 'PROCESSING', 'PROCESSING/ERRORS', 'FAILED');

    public function __construct($host, $user, $password, $db, $default_dataset_id, $api_version, $live_host, $live_user, $live_password, $live_db, $filter_db_host, $filter_db_user, $filter_db_password, $filter_db_db,$iceprod_tok) {
        $this->mysql = @new mysqli($host, $user, $password, $db);
        $this->live = @new mysqli($live_host, $live_user, $live_password, $live_db);
        $this->filter_db = @new mysqli($filter_db_host, $filter_db_user, $filter_db_password, $filter_db_db);
        $this->result = array('api_version' => $api_version,'error' => 0, 'error_msg' => '', 'error_trace' => '', 'data' => array('runs' => array(), 'datasets' => array(), 'seasons' => array()));
        $this->dataset_id = $default_dataset_id;
        $this->default_dataset_id = $default_dataset_id;
        $this->dataset_ids = null;
        $this->dataset_list_only = false;
        $this->pass2 = false;
        $this->rest = new Rest("https://iceprod2-api.icecube.wisc.edu",$iceprod_tok);
        $this->tasks = null;
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

        return array('category' => $basic, 'status' => $status, 'value' => intval($value));
    }

    public function execute() {
        $run_pattern = array('run_id' => null, 'sub_runs' => -1, 'date' => null, 'status' => null,
                             'jobs_states' => array(), 'jobs_prev_states' => array(), 'failures' => array(), 'validated' => false,
                             'submitted' => false, 'last_status_change' => '', 'error_message' => array(),
                             'snapshot_id' => null, 'production_version' => null, 'path' => null, 'gcd' => null);

        $this->add_dataset_list();   
        $this->add_season_list();

        if(!$this->dataset_list_only && !$this->result['error']) {
            $this->add_good_run_list($run_pattern);
            $this->add_submitted_runs($run_pattern);
            $this->add_paths();
            $this->validate_runs();
            $this->set_run_states();
            $this->add_error_logs();
        }

        return $this->result;
    }

    private function add_paths() {

        if ($this->iceprod_id) {
        	return $this->add_paths_ip2();
        }
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

            $path = Tools::remove_path_prefix($path);

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
                $this->result['data']['runs'][$run['run_id']]['gcd'] = Tools::join_paths(Tools::remove_path_prefix($run['path']), $run['name']);
            }
        }
    }

    private function add_paths_ip2() {
        // Add paths to run folder (L2/3)

        $sql = "SELECT run_id, sub_run, task_id
                FROM dataset_subruns r
                WHERE r.dataset_id = {$this->dataset_id}";

        $query = $this->filter_db->query($sql);
        $data = array(); 
        $run_id = NULL;
        while($run = $query->fetch_assoc()) { 
		$paths = array();
		if ($run['run_id'] == $run_id)
		{
			continue;
		} 
		$run_id = $run['run_id'];
		$task_id = $run['task_id'];
		$url = "/datasets/{$this->iceprod_id}/files/{$task_id}"; 

		$response = $this->rest->httpGet($url, $data); 
		$files = (array) json_decode($response,true);
		foreach($files['files'] as $tfile) {
			if ( (strcmp($tfile['movement'], 'input') == 0) && ( preg_match ( "/GCD/", $tfile['remote']) )) 
			{
				$gcdpath = $tfile['remote'];
			} 
			else if ( strcmp($tfile['movement'], 'output') == 0)
			{ 
				$path[] = $tfile['remote'];
			}
		}
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

            $path = Tools::remove_path_prefix($path);

            // Add it to the list:
            if(isset($this->result['data']['runs'][$run['run_id']])) {
                $this->result['data']['runs'][$run['run_id']]['path'] = $path;
                $this->result['data']['runs'][$run['run_id']]['gcd'] = $gcdpath;
            }
        }

            if(isset($this->result['data']['runs'][$run['run_id']])) {
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
                FROM i3filter.seasons
                ORDER BY season";

        $query = $this->filter_db->query($sql);
        while($season = $query->fetch_assoc()) {
            $season['test_runs'] = strlen($season['test_runs']) === 0 ? array() : array_map(function($v) {return intval(trim($v));}, explode(',', $season['test_runs']));
            $season['first_run'] = intval($season['first_run']);
            $season['season'] = intval($season['season']);
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
            if((isset($run['jobs_states']['ERROR']) ? $run['jobs_states']['ERROR'] : 0) + (isset($run['jobs_states']['FAILED']) ? $run['jobs_states']['FAILED'] : 0) > 0) {
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
        $dataset_info = $this->result['data']['datasets'][$this->dataset_id];
        $season = $dataset_info['season'];

        // If it is a L3 dataset and earlier than 1885, there is no validation tag
        // So, mark all runs as validated
        // Same for L2 datasets of season 2010 and 2012 AND not pass2
        if(($dataset_info['type'] == 'L3' && $this->dataset_id < 1885) || (!$this->pass2 && $dataset_info['type'] == 'L2' && ($season == 2010 || $season == 2012))) {
            foreach($this->result['data']['runs'] as &$run) {
                $run['validated'] = true;
                $run['validation_date'] = null;
            }

            return;
        }

        // L2 runs before season 2017 (dataset_id 1915) were actually store on dbs4 in grl_snapshot_info along with the validation information
        // This data has been migrated but w/o dataset_id information. The dataset_id for those runs in post_processing is '0'
        $pp_dataset_id = $this->dataset_id;

        if($dataset_info['type'] == 'L2' && $this->dataset_id < 1914) {
            $pp_dataset_id = 0;
        }

        $sql = "SELECT run_id, validated, date_of_validation
                FROM i3filter.post_processing
                WHERE dataset_id = {$pp_dataset_id}";

        $query = $this->filter_db->query($sql);
        while($row = $query->fetch_assoc()) {
            if(isset($this->result['data']['runs'][$row['run_id']])) {
                $this->result['data']['runs'][$row['run_id']]['validated'] = $row['validated'] == 1;
                $this->result['data']['runs'][$row['run_id']]['validation_date'] = $row['date_of_validation'];
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

        // Holds run information whatever the data source is
        $runs = array();

        if($season >= 2010) {
            // If it is pass2, we have a different table and DB
            $db = $this->filter_db;
            $table = 'i3filter.runs';

            $sql = "SELECT  run_id,
                            DATE(good_tstart) AS `date`,
                            snapshot_id AS `snapshot_id`,
                            production_version AS `production_version`,
                            good_i3,
                            good_it
                    FROM {$table}
                    WHERE   (
                                run_id BETWEEN $first_run_id AND $last_run_id OR
                                run_id IN ($season_test_runs -1) /* Have at least -1 to avoid bad SQL */
                            ) AND
                            run_id NOT IN ($next_season_test_runs -1) /* Have at least -1 to avoid bad SQL */
                    GROUP BY run_id, production_version
                    ORDER BY run_id, production_version ASC";

            if($this->pass2) {
                $table = 'i3filter.grl_snapshot_info_pass2';
                $db = $this->mysql;
                $sql = "SELECT  run_id,
                                validated,
                                DATE(good_tstart) AS `date`,
                                submitted,
                                snapshot_id AS `snapshot_id`,
                                production_version AS `production_version`,
                                good_i3,
                                good_it
                        FROM {$table}
                        WHERE   (
                                    run_id BETWEEN $first_run_id AND $last_run_id OR
                                    run_id IN ($season_test_runs -1) /* Have at least -1 to avoid bad SQL */
                                ) AND
                                run_id NOT IN ($next_season_test_runs -1) /* Have at least -1 to avoid bad SQL */
                        GROUP BY run_id, production_version
                        ORDER BY run_id, production_version ASC";
            }

            $query = $db->query($sql);
            while($row = $query->fetch_assoc()) {
                $runs[] = $row;
            }
        } else {
            // not supported
            throw new Exception("Season $season is not supported.");
        }

        // Putting all together
        foreach($runs as $run) {
            $test_run24h = in_array($run['run_id'], $season_info['test_runs']);

            $this->process_good_run_information($run_pattern, $test_run24h, $run['good_i3'], $run['good_it'], $run['run_id'], $run['production_version'], $run['snapshot_id'], $run['date']);

            // Store run of GRL
            // If it is a bad run (make sure that the SQL query has an order of production_version ASC and snapshot_id ASC), remove it
            if(!intval($run['good_it']) && !intval($run['good_i3']) && !in_array($run['run_id'], $season_info['test_runs'])) {
                $key = array_search($run['run_id'], $grl_run_ids);
                if($key !== false) {
                    unset($grl_run_ids[$key]);
                }
            } else {
                $grl_run_ids[] = $run['run_id'];
            }
        }

        // Remove all runs from result run list that are not in the GRL
        foreach($this->result['data']['runs'] as $run_id => &$value) {
            if(!in_array($run_id, $grl_run_ids)) {
                unset($this->result['data']['runs'][$run_id]);
            }
        }
    }

    private function process_good_run_information($run_pattern, $test_run24h, $good_i3, $good_it, $run_id, $production_version, $snapshot_id, $date) {
        $dataset_info = $this->result['data']['datasets'][$this->dataset_id];

        if(isset($this->result['data']['runs'][$run_id])) {
            // A run can appear several times (each for one production or snapshot number that includes this run).
            // Therefore, if the run already exists, check if the production/snaphot number is newer (higher)...
            if(!is_null($this->result['data']['runs'][$run_id]['production_version']) && $this->result['data']['runs'][$run_id]['production_version'] > $production_version) {
                return;
            }

            if($this->result['data']['runs'][$run_id]['snapshot_id'] > $snapshot_id) {
                return;
            }

            // Add production version and snapshot id
            $this->result['data']['runs'][$run_id]['production_version'] = $production_version;
            $this->result['data']['runs'][$run_id]['snapshot_id'] = $snapshot_id;
            $this->result['data']['runs'][$run_id]['good_i3'] = (bool)$good_i3;
            $this->result['data']['runs'][$run_id]['good_it'] = (bool)$good_it;
            $this->result['data']['runs'][$run_id]['24h_test_run'] = $test_run24h;
        } else {
            $current_run = $run_pattern;
        
            $current_run['run_id'] = $run_id;
            $current_run['date'] = $date;
            $current_run['submitted'] = false;
            $current_run['status'] = self::get_status('NONE');
            $current_run['production_version'] = intval($production_version);
            $current_run['snapshot_id'] = intval($snapshot_id);
            $current_run['24h_test_run'] = $test_run24h;
            $current_run['good_i3'] = (bool)$good_i3;
            $current_run['good_it'] = (bool)$good_it;
        
            $this->result['data']['runs'][$run_id] = $current_run;
        }
    }

    private function add_submitted_runs(array &$run_pattern) {

        if (intval($this->dataset_id) > 20000) {
        	return $this->add_submitted_runs_ip2($run_pattern);
        }
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

            // Set correct datatype
            $current_run['sub_runs'] = intval($current_run['sub_runs']);

            // Add run
            $this->result['data']['runs'][$row['run_id']] = $current_run;
        }
    }

    private function get_tasks() { 
	    if (is_null($this->tasks)) { 
		$url = "/datasets/{$this->iceprod_id}/tasks"; 
		$data = array('status'=>'processing'); 
		$response = $this->rest->httpGet($url, $data); 
		$this->tasks = (array) json_decode($response,true);
	    } 
	    return $this->tasks;
    }

    private function get_task($task_id) { 
	   $url = "/datasets/{$this->iceprod_id}/tasks/{$task_id}"; 
	   $data = array(); 
	   $response = $this->rest->httpGet($url, $data); 
	   return (array) json_decode($response,true);
    }





    private function add_submitted_runs_ip2(array &$run_pattern) {
        $this->get_source_dataset_ids();

        //$tasks = $this->get_tasks();

        $sql = "SELECT  r.run_id, 
                        COUNT(sub_run) AS `sub_runs`, 
                        date, status
                FROM dataset_subruns r
                WHERE   r.dataset_id = {$this->dataset_id}
                GROUP BY r.run_id
                ORDER BY r.run_id ASC";
 
        $query = $this->filter_db->query($sql);
        while($row = $query->fetch_assoc()) {
            //$current_run = $run_pattern;

            $current_run = $this->result['data']['runs'][$row['run_id']];
            $all_jobs_states = 0;

	    $job_states = array(); 
	    $job_prev_states = array(); 
            $failures = array();

	    foreach(self::$JOB_STATES as $job_status) { 
		   $job_states[$job_status] = 0;
		   $job_prev_states[$job_status] = 0;
	    }



            $sql = "SELECT  r.run_id, date, r.task_id, r.status, r.sub_run
                FROM dataset_subruns r
                WHERE   r.dataset_id = {$this->dataset_id}
                AND r.run_id = {$row['run_id']}";
             $subquery = $this->filter_db->query($sql);
             while($subrun = $subquery->fetch_assoc()) {
		   $task_id = $subrun['task_id'];
		   //$task = $tasks[$task_id];
		   //$task = $this->get_task($task_id);
		   $task = json_decode('{"db433e30c07511ea8747141877284d92": {"dataset_id": "b5d62ef2c07311ea8747141877284d92", "job_id": "db3b7826c07511eaa037141877284d92", "task_index": 0, "job_index": 0, "name": "filtering", "depends": [], "requirements": {"memory": 3, "os": ["RHEL_7_x86_64"]}, "task_id": "db433e30c07511ea8747141877284d92", "status_changed": "2020-07-07T17:55:43.508761", "failures": 0, "evictions": 0, "walltime": 0.55, "walltime_err": 0.0, "walltime_err_n": 0, "site": "MSU", "status": "complete", "priority": 1.0}');
		   $task_status = strtoupper( $task['status'] );
		   if (strcmp($task_status, 'COMPLETE') == 0) {
			   $job_states['OK'] += 1;
		   } else { 
			   $job_states[$task_status] += 1;
		   }
		   $all_jobs_states += 1;
		   $job_prev_states[$subrun['status']] += 1;
		   if (isset($task['status_changed'])) {
		   	$current_run['last_status_change'] = $task['status_changed'];
		   }
                   $failures[] = array('sub_run' => $subrun['sub_run'], 'failures' => $task['failures'], 'job_id' => $task['job_index']);
	     }

            // This run is obviously submitted
            $current_run['submitted'] = true;
            $current_run['date'] = $row['date'];

            $current_run['run_id'] = $row['run_id'];
            //$current_run['validated'] = true;

            $current_run['sub_runs'] = intval($row['sub_runs']);
            $current_run['failures'] = $failures;
            $current_run['jobs_states'] = $job_states;
            $current_run['error_message'] = $failures;
            $current_run['jobs_prev_states'] = $job_prev_states;
            $current_run['path'] = 'some_path';
            $current_run['gcd'] = 'some_gcd';
            if (count($job_states['COMPLETED']) == $all_jobstates ) {
                $current_run['status'] = self::get_status('OK');
	    }

            $current_run['sub_runs'] = intval($row['sub_runs']);

            $this->result['data']['runs'][$row['run_id']] = $current_run; 
	} 
    }


    private function get_source_dataset_ids() {
        $sql = "SELECT * FROM i3filter.source_dataset_id";

        $query = $this->filter_db->query($sql);
        $parents = array();
        while($row = $query->fetch_assoc()) {
            $row['dataset_id'] = intval($row['dataset_id']);
            $row['source_dataset_id'] = intval($row['source_dataset_id']);

            if(!isset($parents[$row['dataset_id']])) {
                $parents[$row['dataset_id']] = array();
            }

            $parents[$row['dataset_id']][] = $row['source_dataset_id'];
        }

        return $parents;
    }

    public function get_dataset_ids() {
        if(is_null($this->dataset_ids)) {
            $source_dataset_ids = $this->get_source_dataset_ids();

            $list = array();

            $sql = 'SELECT * FROM i3filter.datasets d JOIN i3filter.working_groups wg ON d.working_group = wg.working_group_id';

            $query = $this->filter_db->query($sql);
            while($row = $query->fetch_assoc()) {
                $set = array();

                $set['dataset_id'] = intval($row['dataset_id']);
                $set['description'] = $row['description'];
                $set['selected'] = $row['dataset_id'] == $this->dataset_id;
                $set['supported'] = (bool)($row['enabled']);
                $set['season'] = intval($row['season']);
                $set['type'] = $row['type'];
                $set['comment'] = $row['comment'];
                $set['pass'] = intval($row['pass']);
                $set['status'] = array(
                    'name' => $row['status'],
                    'date' => $row['status_date'],
                    'comment' => $row['status_comment']
                );

                if(isset($source_dataset_ids[$set['dataset_id']])) {
                    $set['source_dataset_ids'] = $source_dataset_ids[$set['dataset_id']];
                } else {
                    $set['source_dataset_ids'] = array();
                }

                if(intval($row['working_group']) > 0) {
                    $set['working_group'] = $row['name'];
                }

                $list[$row['dataset_id']] = $set;
            }
            
            $this->dataset_ids = $list;
        }

        return $this->dataset_ids;
    }

    public function set_dataset_id($dataset_id) {
        $dataset_id = intval($dataset_id);
        $this->dataset_id = $dataset_id;
        if ($this->dataset_id > 20000) { 
		$sql = "SELECT `iceprod_id` FROM i3filter.datasets WHERE dataset_id = {$this->dataset_id}"; 
		$fetch = $this->filter_db->query($sql)->fetch_assoc(); 
		$this->iceprod_id = $fetch['iceprod_id']; 
	}

        $this->add_dataset_list();   
        $this->add_season_list();

        if(isset($this->result['data']['datasets'][(string)$dataset_id]) && $this->result['data']['datasets'][(string)$dataset_id]['supported']) {
            // Set current selection to false
            if($this->dataset_id > 0) {
                $this->result['data']['datasets'][(string)$this->dataset_id]['selected'] = false;
            }

            $this->dataset_id = $dataset_id;

            $sql = "SELECT `iceprod_id` FROM i3filter.datasets WHERE dataset_id = {$this->dataset_id}";
	    $fetch = $this->filter_db->query($sql)->fetch_assoc();
	    $this->iceprod_id = $fetch['iceprod_id']; 

            // Check if pass2
            $this->pass2 = $this->result['data']['datasets'][(string)$this->dataset_id]['pass'] == 2;

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
        if ($this->iceprod_id) {
        	return $this->get_error_jobs_and_msgs_ip2($run);
        }
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



    private function get_error_jobs_and_msgs_ip2($run) {
       // BOOKMARK
       $sql = " SELECT  submitdir,
                        task_id,
                        status,
                        sub_run,
                        r.queue_id,
                FROM    dataset_subruns r
                WHERE   j.dataset_id = {$this->dataset_id}
                        AND r.run_id = $run;";

        //$tasks = $this->get_tasks();

        $query = $this->filter_db->query($sql);


        $result = array('ERROR' => array(), 'FAILED' => array());
        while($job = $query->fetch_assoc()) { 
		$task_id = $job['task_id']; 
		//$task = $tasks[$task_id];
		$task = $this->get_task($task_id);
		$task_status = strtoupper( $task['status'] );
		if ((strcmp($task_status, 'FAILED') == 0) || (strcmp($task_status, 'RESET') == 0) ) { 

			$url = "/datasets/{$this->iceprod_id}/tasks/{$task_id}/logs"; 
			$data = array(); 
			$response = $this->rest->httpGet($url, $data); 
			$logs = (array) json_decode($response,true); 

			$result[$task_status][] = array('job_id' => $task_id,
                                              'sub_run' => $job['sub_run'],
                                              'submitdir' => 'N/A',
                                              'log_tails' => $logs);

		}
        }

        return $result;
    }

    public function set_dataset_list_only($only) {
        $this->dataset_list_only = (bool)$only;
    }

    private static function get_status($name) {
        $ids = array_keys(self::$RUN_STATUS, $name);

        if(count($ids) === 0) {
            throw new InvalidArgumentException("$name does not exists in JOB_STATUS.");
        }

        return array('value' => $ids[0], 'name' => $name);
    }
}
