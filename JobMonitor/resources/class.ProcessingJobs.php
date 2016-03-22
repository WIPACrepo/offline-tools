<?php

class ProcessingJobs {
    private $mysql;
    private $result;
    private $logfile_extensions;
    private $tail_use_lines;
    private $dataset_id;
    private $completed_job_length;
    private $query_current_jobs;
    private $query_completed_jobs;
    private $query_calendar;
    private $dataset_ids;

    public function __construct($host, $user, $password, $db, $tail_use_lines = 10) {
        $this->mysql = @new mysqli($host, $user, $password, $db);
        $this->result = array('error' => 0, 'error_msg' => '', 'data' => array(), 'md5' => array());
        $this->tail_use_lines = $tail_use_lines;
        $this->dataset_id = 1883; // default
        $this->completed_job_length = 10; // default
        $this->logfile_extensions = array('log', 'condor', 'err', 'out');
        $this->query_current_jobs = true;
        $this->query_completed_jobs = true;
        $this->query_calendar = true;
        $this->dataset_ids = null;
    }

    public function get_dataset_ids() {
        if(is_null($this->dataset_ids)) {
            $list = array();
            $sql = 'SELECT dataset_id, description FROM dataset ORDER BY dataset_id DESC';
            $query = $this->mysql->query($sql);
            while($row = $query->fetch_assoc()) {
                $list[] = $row;
            }
            
            $this->dataset_ids = $list;
        }

        return $this->dataset_ids;
    }

    public function set_completed_job_length($length) {
        $length = intval($length);
        if($length > 0) {
            $this->completed_job_length = $length;
        }
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

    public function set_query_current_jobs($b) {
        $this->query_current_jobs = (bool)$b;
    }

    public function set_query_completed_jobs($b) {
        $this->query_completed_jobs = (bool)$b;
    }

    public function set_query_calendar($b) {
        $this->query_calendar = (bool)$b;
    }

    public function execute() {
        if($mysql->connect_error) {
            $this->result['error'] = 1;
            $this->result['error_msg'] = 'Connection failed: ' . $this->connect_error;
        } else {
            if($this->query_current_jobs) {
                $this->result['data']['current'] = array();

                // Incompleted jobs
                $query = $this->query_jobs(false);

                while($row = $query->fetch_assoc()) {
                    $this->add_entry($row, 'current');
                }
            }

            if($this->query_completed_jobs) {
                $this->result['data']['completed'] = array();
                // Completed jobs
                $query = $this->query_jobs(true);

                while($row = $query->fetch_assoc()) {
                    $this->add_entry($row, 'completed');
                }
            }

            if($this->query_calendar) {
                $this->create_calendar();
            }
        }

        return $this->result;
    }

    private function calendar_get_good_runs($startdate) {
        $sql = "SELECT GROUP_CONCAT(DISTINCT run_id) AS run_ids,
                       COUNT(*) AS runs,
                       DATE(good_tstart) AS `date`
                FROM grl_snapshot_info
                WHERE   good_tstart >= '{$this->mysql->real_escape_string($startdate)}'
                        AND (good_i3 = 1 OR good_it = 1)
                GROUP BY `date` 
                ORDER BY `date` ASC";

        $query = $this->mysql->query($sql);

        $grl = array();
        while($day = $query->fetch_assoc()) {
            $grl[$day['date']] = array(explode(',', $day['run_ids']),
                                       $day['runs']);
        }

        return $grl;
    }

    private function calendar_get_not_validated_runs($startdate) {
        if(is_null($startdate)) {
            throw new InvalidArgumentException('Invalid argument $startdate.');
        }

        $sql = "SELECT run_id
                FROM grl_snapshot_info
                WHERE   good_tstart >= '{$this->mysql->real_escape_string($startdate)}'
                        AND (good_i3 = 1 OR good_it = 1)
                        AND validated = 0";

        $query = $this->mysql->query($sql);
    
        $not_validated_runs = array();

        while($run = $query->fetch_assoc()) {
            $not_validated_runs[] = $run['run_id'];
        }

        return $not_validated_runs;
    }

    private function calendar_get_proc_runs() {
        $PROC_STATES = explode(',', 'IDLE,WAITING,QUEUEING,QUEUED,PROCESSING,READYTOCOPY,COPYING,SUSPENDED,RESET,COPIED,EVICTED,CLEANING');

        $sql = "SELECT DISTINCT run_id FROM run r
                JOIN job j
                    ON r.queue_id = j.queue_id
                    AND r.dataset_id = j.dataset_id
                WHERE r.dataset_id = {$this->mysql->real_escape_string($this->dataset_id)}";

        $first = true;
        $sql .= ' AND (';
        foreach($PROC_STATES as $status) {
            if($first) {
                $first = false;
            } else {
                $sql .= ' OR ';
            }
            $sql .= "status='$status'";
        }

        $sql .= ')';

        $query = $this->mysql->query($sql);
    
        $proc = array();

        while($run = $query->fetch_assoc()) {
            $proc[] = $run['run_id'];
        }

        return $proc;
    }

    private function calendar_get_proc_error_runs() {
        $sql = "SELECT DISTINCT run_id FROM run r
                JOIN job j
                    ON r.queue_id = j.queue_id
                    AND r.dataset_id = j.dataset_id
                WHERE (status = 'FAILED' OR status = 'ERROR')
                    AND r.dataset_id = {$this->mysql->real_escape_string($this->dataset_id)}";

        $query = $this->mysql->query($sql);
    
        $proc_err = array();

        while($run = $query->fetch_assoc()) {
            $proc_err[] = $run['run_id'];
        }

        return $proc_err;
    }

    private function create_calendar() {
       $sql = " SELECT  GROUP_CONCAT(DISTINCT run_id) AS `run_ids`,
                        COUNT(sub_run) AS subruns, 
                        date,
                        COUNT(*) AS `all`, 
                        SUM(IF(status = 'OK', 1, 0)) AS `OK`, 
                        SUM(IF(status = 'ERROR', 1, 0)) AS `ERROR`, 
                        SUM(IF(status = 'FAILED', 1, 0)) AS `FAILED`,
                        SUM(IF(status = 'BadRun', 1, 0)) AS `BadRun`,
                        SUM(IF(status = 'IDLEShortRun', 1, 0)) AS `IDLEShortRun`,
                        SUM(IF(status = 'IDLENoFiles', 1, 0)) AS `IDLENoFiles`,
                        SUM(IF(status = 'IDLETestRun', 1, 0)) AS `IDLETestRun`,
                        SUM(IF(status = 'IDLELid', 1, 0)) AS `IDLELid`,
                        SUM(IF(status = 'IDLEBDList', 1, 0)) AS `IDLEBDList`,
                        SUM(IF(status = 'IDLEIncompleteFiles', 1, 0)) AS `IDLEIncompleteFiles`,
                        SUM(IF(status = 'IDLENoGCD', 1, 0)) AS `IDLENoGCD`,
                        SUM(IF(status = 'FailedRun', 1, 0)) AS `FailedRun`
                FROM    run r 
                JOIN    job j 
                    ON j.queue_id = r.queue_id 
                    AND r.dataset_id = j.dataset_id 
                WHERE r.dataset_id = {$this->mysql->real_escape_string($this->dataset_id)} 
                GROUP BY date 
                ORDER BY date ASC";

        $calendar = array();
        $grl = null;

        $status_list = explode(',', 'OK,ERROR,FAILED,BadRun,IDLEShortRun,IDLENoFiles,IDLETestRun,IDLELid,IDLEBDList,IDLEIncompleteFiles,IDLENoGCD,FailedRun');

        $start_date = null;
        $query = $this->mysql->query($sql);
        while($day = $query->fetch_assoc()) {
            if(is_null($grl)) {
                $grl = $this->calendar_get_good_runs($day['date']);
                $start_date = $day['date'];
            }

            $date = date_parse($day['date']);

            if(!array_key_exists($date['year'], $calendar)) {
                $calendar[$date['year']] = array();
            }

            if(!array_key_exists($date['month'], $calendar)) {
                $calendar[$date['month']] = array();
            }

            $today = array( 'grl' => $grl[$day['date']][0],
                            'submitted_runs' => explode(',', $day['run_ids']),
                            'jobs' => intval($day['all']),
                            );

            foreach($status_list as $status) {
                $today[$status] = intval($day[$status]);
            }

            $calendar[$date['year']][$date['month']][$date['day']] = $today;
        }

        $this->result['data']['calendar'] = $calendar;
        $this->result['data']['calendar']['not_validated'] = $this->calendar_get_not_validated_runs($start_date);
        $this->result['data']['calendar']['proc_error'] = $this->calendar_get_proc_error_runs();
        $this->result['data']['calendar']['proc'] = $this->calendar_get_proc_runs();

        $this->result['md5']['calendar'] = md5(json_encode($this->result['data']['calendar'])); 
    }

    private function add_entry($row, $type) {
        $row['extended_info'] = array('failed' => array(), 'error' => array());

        if($row['num_status_failed'] + $row['num_status_error'] > 0) {
            $fails = $this->get_error_jobs_and_msgs($row['run_id']);
            $row['extended_info']['failed'] = $fails['FAILED'];
            $row['extended_info']['error'] = $fails['ERROR'];
        }

        if($row['prev_state'] == 'NULL') {
            $row['prev_state'] = '';
        }

        $row['status'] = explode(',', $row['status']);
        $row['prev_state'] = explode(',', $row['prev_state']);

        $this->result['data'][$type][] = $row;
        $this->result['md5'][$type] = md5(json_encode($this->result['data'][$type]));
    }

    private function query_jobs($completed_jobs = false) {
        $sql = "SELECT  r.run_id,
            SUM(IF(status = 'OK', 1, 0)) AS num_status_ok,
            SUM(IF(status = 'PROCESSING', 1, 0)) AS num_status_processing,
            SUM(IF(status = 'ERROR', 1, 0)) AS num_status_error,
            SUM(IF(status = 'FAILED', 1, 0)) AS num_status_failed,
            COUNT(run_id) AS num_of_jobs,
            GROUP_CONCAT(DISTINCT status ORDER BY status ASC) AS status,
            GROUP_CONCAT(DISTINCT prev_state ORDER BY status ASC) AS prev_state,
            GROUP_CONCAT(DISTINCT failures ORDER BY failures DESC) AS failures,
            GROUP_CONCAT(DISTINCT evictions ORDER BY evictions DESC) AS evictions,
            MAX(status_changed) AS last_status_change,
            date
        FROM i3filter.job j
        JOIN i3filter.run r ON j.queue_id=r.queue_id 
        WHERE   j.dataset_id={$this->dataset_id} AND 
                r.dataset_id={$this->dataset_id}
        GROUP BY r.run_id";

        if($completed_jobs) {
            $sql .= "
                HAVING  status = 'OK'
                ORDER BY last_status_change DESC
                LIMIT {$this->completed_job_length};";
        } else {
            $sql .= "
            HAVING  status NOT LIKE 'OK' AND 
                status NOT LIKE 'IDLETestRun' AND 
                status NOT LIKE 'FailedRun' AND 
                status NOT LIKE 'IDLEShortRun' AND 
                status NOT LIKE 'OK,BadRun' AND 
                status NOT LIKE 'BadRun';";        
        }

        return $this->mysql->query($sql);
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
                        j.queue_id,
                        errormessage 
                FROM    i3filter.job j
                JOIN    i3filter.run r 
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
                                              'submitdir' => $job['submitdir'],
                                              'log_tails' => $logs);
        }

        return $result;
    }

    /* Found here: https://gist.github.com/lorenzos/1711e81a9162320fde20
     * Benchmarks: http://stackoverflow.com/a/15025877                                                                                                                                        
     */
    private function tail($filepath, $lines = null, $adaptive = true) {
        if(is_null($lines)) {
            $lines = $this->tail_use_lines;
        }

        // Open file
        $f = @fopen($filepath, "rb");
        if ($f === false) return false;
        // Sets buffer size
        if (!$adaptive) $buffer = 4096;
        else $buffer = ($lines < 2 ? 64 : ($lines < 10 ? 512 : 4096));
        // Jump to last character
        fseek($f, -1, SEEK_END);
        // Read it and adjust line number if necessary
        // (Otherwise the result would be wrong if file doesn't end with a blank line)
        if (fread($f, 1) != "\n") $lines -= 1;
        
        // Start reading
        $output = '';
        $chunk = '';
        // While we would like more
        while (ftell($f) > 0 && $lines >= 0) {
            // Figure out how far back we should jump
            $seek = min(ftell($f), $buffer);
            // Do the jump (backwards, relative to where we are)
            fseek($f, -$seek, SEEK_CUR);
            // Read a chunk and prepend it to our output
            $output = ($chunk = fread($f, $seek)) . $output;
            // Jump back to where we started reading
            fseek($f, -mb_strlen($chunk, '8bit'), SEEK_CUR);
            // Decrease our line counter
            $lines -= substr_count($chunk, "\n");
        }
        // While we have too many lines
        // (Because of buffer size we might have read too many)
        while ($lines++ < 0) {
            // Find first newline and remove all text before that
            $output = substr($output, strpos($output, "\n") + 1);
        }
        // Close file and return
        fclose($f);
        return trim($output);
    }
}
