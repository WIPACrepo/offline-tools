<?php

require_once('class.ProcessingJobs.php');

class Search {
    private $run_id;
    private $event_id;

    private $mysql;
    private $result;

    private $api_version;

    private $datawarehouse_prefix;
   
    public static $result_pattern = array('api_version' => null,'error' => 0, 'error_msg' => '', 'error_trace' => '', 'data' => array('query' => array(), 'result' => null));
 
    public function __construct($host, $user, $password, $db, $datawarehouse_prefix, $api_version) {
        $this->mysql = @new mysqli($host, $user, $password, $db);
        $this->result = self::$result_pattern;

        $this->result['api_version'] = $api_version;

        $this->run_id = null;
        $this->event_id = null;

        $this->datawarehouse_prefix = $datawarehouse_prefix;
    }

    public function set_run_id($run_id) {
        $this->run_id = intval($run_id);
        if($this->run_id < 0 || !is_numeric($run_id)) {
            throw new InvalidArgumentException("'$run_id' is not a valid run id"); 
        }
    }

    public function set_event_id($event_id) {
        $this->event_id = intval($event_id);
        if($this->event < 0 || !is_numeric($event_id)) {
            throw new InvalidArgumentException("'$event_id' is not a valid event id"); 
        }
    }

    private function get_gaps_files($run_id) {
        $files = array();

        $sql = "SELECT run_id, sub_run, path, name 
                FROM urlpath u 
                JOIN run r 
                    ON r.queue_id = u.queue_id 
                    AND r.dataset_id = u.dataset_id 
                WHERE run_id = $run_id 
                    AND type = 'PERMANENT' 
                    AND path LIKE 'file:/data/exp/IceCube/____/filtered/level2/%' 
                    AND name LIKE '%gaps.txt'";

        $query = $this->mysql->query($sql);
        while($path = $query->fetch_assoc()) {
            $files[] = ProcessingJobs::join_paths(substr($path['path'], 5), $path['name']);
        }

        return $files;
    }

    private function get_subrun_by_event_id($event_id) {
        $gaps_files = $this->get_gaps_files($this->run_id);

        $first_event_key = 'First Event of File';
        $last_event_key = 'Last Event of File';

        // Sort files in order to stop searching if event number is smaller than gaps file event numbers
        sort($gaps_files);

        foreach($gaps_files as &$file) {
            $content = file($this->datawarehouse_prefix . $file);

            $tmp = implode($content);

            $first_event = -1;
            $last_event = -1;
            foreach($content as &$line) {
                $split = explode(':', $line);

                if(count($split) != 2) {
                    continue;
                }

                if(trim($split[0]) === $first_event_key) {
                    $first_event = $this->get_event($split[1]);
                } else if(trim($split[0]) === $last_event_key) {
                    $last_event = $this->get_event($split[1]);
                }
            }

            if($first_event !== -1 && $last_event !== -1) {
                if($event_id >= $first_event && $event_id <= $last_event) {
                    return $file;
                } else if($event_id < $last_event) {
                    return false;
                }
            }
        }

        return false;
    }

    private function get_event($str) {
        $event = -1;

        $split = explode(' ', trim($str));

        if(is_numeric(trim($split[0]))) {
            $event = intval(trim($split[0]));
        }

        return $event;
    }

    public function execute() {
        if(is_null($this->run_id) && !is_null($this->event_id)) {
            throw new InvalidArgumentException('Run id is required for event search');
        }

        if(is_null($this->run_id)) {
            throw new InvalidArgumentException('Run id is required');
        }

        // Search modes
        if(!is_null($this->run_id) && !is_null($this->event_id)) {
            // Search for event id
            $this->result['data']['query'] = array('run_id' => $this->run_id, 'event_id' => $this->event_id);

            $gaps_file = $this->get_subrun_by_event_id($this->event_id);

            $this->result['data']['result'] = array('successfully' => false,
                                                    'message' => "Event {$this->event_id} was not found in run {$this->run_id}",
                                                    'sub_run' => null,
                                                    'file' => null);

            if(false !== $gaps_file) {
                $this->result['data']['result']['successfully'] = true;
                $this->result['data']['result']['file'] = substr($gaps_file, 0, -9) . '.i3.bz2';
                $this->result['data']['result']['sub_run'] = intval(substr($gaps_file, -17, 8));
                $this->result['data']['result']['message'] = "Event {$this->event_id} was successfully found in sub run {$this->result['data']['result']['sub_run']} of run {$this->run_id}";
            }
        } else {
            // Search for run id
        }

        return $this->result;
    }
}
