<?php

class ProcessingJobs {
    private $mysql;
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
    private static $RUN_STATUS = array('NONE', 'IDLE', 'OK', 'PROCESSING', 'PROCESSING/ERRORS', 'FAILED');

    public function __construct($host, $user, $password, $db, $default_dataset_id, array $l2_dataset_ids) {
        $this->mysql = @new mysqli($host, $user, $password, $db);
        $this->result = array('error' => 0, 'error_msg' => '', 'data' => array('runs' => array(), 'datasets' => array()));
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
                             'submitted' => false, 'last_status_change' => '', 'error_message' => array());

        $this->is_l2_dataset = in_array($this->dataset_id, $this->l2_dataset_ids);

        if(!$this->dataset_list_only) {
            $this->add_submitted_runs($run_pattern);
            $this->add_good_run_list($run_pattern);
            $this->validate_runs();
            $this->set_run_states();
            $this->add_error_logs();
        }

        $this->add_dataset_list();   

        return $this->result;
    }

    private function add_dataset_list() {
        $this->result['data']['datasets'] = $this->get_dataset_ids();
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
                    $status = self::get_status('PROCESSING/ERROR');
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
        // Get smallest run number from submitted jobs
        // It is the first element in the result array
        // since it is ordered by run_id ASC and PHP
        // arrays are ordered
        $run_ids = array_keys($this->result['data']['runs']);

        if(count($run_ids) < 1) {
            // No runs for this dataset submitted yet. Do nothing since we don't know
            // from grl_snapshot_info which run belongs to which dataset
            return;
        }

        $first_run_id = $run_ids[0];

        // Biggest run number:
        $last_run_id = $run_ids[count($run_ids) - 1];

        $sql = "SELECT  run_id,
                        validated,
                        DATE(good_tstart) AS `date`,
                        submitted
                FROM grl_snapshot_info
                WHERE   run_id BETWEEN $first_run_id AND $last_run_id AND
                        (good_it = 1 OR good_i3 = 1)
                ORDER BY run_id ASC";

        $query = $this->mysql->query($sql);
        while($row = $query->fetch_assoc()) {
            if(isset($this->result['data']['runs'][$row['run_id']])) {
                // L2 validation flag is currently stored in grl_snapshot_info
                if($this->is_l2_dataset) {
                    $this->result['data']['runs'][$row['run_id']]['validated'] = $row['validated'] == 1;
                }
            } else {
                $current_run = $run_pattern;

                $current_run['run_id'] = $row['run_id'];
                $current_run['date'] = $row['date'];
                $current_run['submitted'] = $this->is_l2_dataset_id && $row['submitted'] == 1;
                $current_run['status'] = self::get_status('NONE');

                $this->result['data']['runs'][ $row['run_id']] = $current_run;
            }
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
                JOIN grl_snapshot_info g
                    ON r.run_id = g.run_id
                WHERE   r.dataset_id = {$this->dataset_id} AND 
                        (good_it = 1 OR good_i3 = 1) AND
                        r.run_id >= 127645
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
            $sql = 'SELECT dataset_id, description FROM dataset ORDER BY dataset_id DESC';
            $query = $this->mysql->query($sql);
            while($row = $query->fetch_assoc()) {
                $row['selected'] = $row['dataset_id'] == $this->dataset_id;

                $list[] = $row;
            }
            
            $this->dataset_ids = $list;
        }

        return $this->dataset_ids;
    }

    public function set_dataset_id($dataset_id) {
        $dataset_id = intval($dataset_id);

        $sql = "SELECT EXISTS(SELECT 1 FROM dataset WHERE dataset_id = $dataset_id) AS `exists`";
        $query = $this->mysql->query($sql);
        $result = $query->fetch_assoc();

        if(intval($result['exists'])) {
            $this->dataset_id = $dataset_id;
            return true;
        } else {
            return false;
        }
    }

    private function parse_errormessage($msg) {
        $msg = str_replace('<br>', '', $msg);
        $parts =  preg_split('/----([_0-9a-z\.\ ]+)----[\:]{0,1}/i', $msg, -1,  PREG_SPLIT_DELIM_CAPTURE);

        #print_r($parts);
        
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

    private static function get_status($name) {
        $ids = array_keys(self::$RUN_STATUS, $name);

        if(count($ids) === 0) {
            throw new InvalidArgumentException("$name does not exists in JOB_STATUS.");
        }

        return array('value' => $ids[0], 'name' => $name);
    }
}
