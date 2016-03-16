<?php

class ProcessingJobs {
    private $mysql;
    private $result;
    private $logfile_extensions;
    private $tail_use_lines;
    private $dataset_id;
    private $completed_job_length;

    public function __construct($host, $user, $password, $db, $tail_use_lines = 10) {
        $this->mysql = @new mysqli($host, $user, $password, $db);
        $this->result = array('error' => 0, 'error_msg' => '', 'data' => array('current' => array(), 'completed' => array()));
        $this->tail_use_lines = $tail_use_lines;
        $this->dataset_id = 1883; // default
        $this->completed_job_length = 10; // default
        $this->logfile_extensions = array('log', 'condor', 'err', 'out');
    }

    public function get_dataset_ids() {
        $list = array();
        $sql = 'SELECT dataset_id, description FROM dataset ORDER BY dataset_id DESC';
        $query = $this->mysql->query($sql);
        while($row = $query->fetch_assoc()) {
            $list[] = $row;
        }

        return $list;
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

    public function execute() {
        if($mysql->connect_error) {
            $this->result['error'] = 1;
            $this->result['error_msg'] = 'Connection failed: ' . $this->connect_error;
        } else {
            // Incompleted jobs
            $query = $this->query_jobs(false);

            while($row = $query->fetch_assoc()) {
                $this->add_entry($row, 'current');
            }

            // Completed jobs
            $query = $this->query_jobs(true);

            while($row = $query->fetch_assoc()) {
                $this->add_entry($row, 'completed');
            }
        }

        return $this->result;
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

    private function get_error_jobs_and_msgs($run) {
       $sql = " SELECT  submitdir,
                        job_id,
                        status,
                        j.queue_id 
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
            $logs = array();

            // Get tails of logfiles
            $files = @scandir($job['submitdir'] . '/');
            if(false !== $files) {
                foreach($files as $file) {
                    // check file extensions
                    $pinfo = pathinfo($file);
                    if(in_array($pinfo['extension'], $this->logfile_extensions)) {
                        // If the file doesn't exists or something went wrong tail returns false
                        $tail = $this->tail($path);
                        $logs[] = array('file' => $path,
                                        'error' => $tail === false,
                                        'content' => $tail);
                    }
                }
            }

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
