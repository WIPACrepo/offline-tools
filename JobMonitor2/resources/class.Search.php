<?php

require_once('class.ProcessingJobs.php');

class Search {
    private $run_id;
    private $event_id;

    private $mysql;
    private $live;
    private $result;

    private $api_version;

    private $datawarehouse_prefix;
   
    public static $result_pattern = array('api_version' => null,'error' => 0, 'error_msg' => '', 'error_trace' => '', 'data' => array('query' => array(), 'result' => null));
 
    public function __construct($host, $user, $password, $db, $datawarehouse_prefix, $api_version, $live_host, $live_user, $live_password, $live_db) {
        $this->mysql = @new mysqli($host, $user, $password, $db);
        $this->live = @new mysqli($live_host, $live_user, $live_password, $live_db);
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

    private function get_paths_and_datasets($run_id) {
        $sql = "SELECT run_id,
                    GROUP_CONCAT(DISTINCT path SEPARATOR '*') AS `paths`,
                    r.dataset_id
                FROM urlpath u 
                JOIN run r
                    ON r.queue_id = u.queue_id 
                    AND r.dataset_id = u.dataset_id 
                WHERE r.run_id = $run_id 
                AND type = 'PERMANENT' 
                GROUP BY run_id, dataset_id";

        $result = array();

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

            $run_id = intval($run['run_id']);
            $dataset_id = intval($run['dataset_id']);

            $result[] = array('dataset_id' => $dataset_id, 'path' => $path);
        }

        return $result;
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
            $date = $path['date']; // It's always the same date
            $files[] = ProcessingJobs::join_paths(substr($path['path'], 5), $path['name']);
        }

        return $files;
    }

    private function get_subrun_by_event_id_and_gaps_files($gaps_files, $event_id) {
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

    private function get_files_for_subrun_and_run($run_id, $sub_run) {
        $sql = "SELECT r.dataset_id, path, name 
                FROM run r 
                JOIN urlpath u 
                    ON r.dataset_id = u.dataset_id 
                    AND r.queue_id = u.queue_id 
                WHERE run_id = $run_id 
                    AND sub_run = $sub_run 
                    AND type = 'PERMANENT'";

        $result = array();

        $query = $this->mysql->query($sql);
        while($info = $query->fetch_assoc()) {
            // Filter EHE and IT and other extensions files
            if(is_numeric(substr($info['name'], -8, 1)) && substr($info['name'], -7) === '.i3.bz2') {
                $result[] = array('dataset_id' => $info['dataset_id'], 'file' => ProcessingJobs::join_paths(substr($info['path'], 5), $info['name']));
            }
        }

        return $result;
    }

    private function get_event($str) {
        $event = -1;

        $split = explode(' ', trim($str));

        if(is_numeric(trim($split[0]))) {
            $event = intval(trim($split[0]));
        }

        return $event;
    }

    private function get_grl_info($run_id) {
        $sql = "SELECT DATE(tstart) AS `date`,
                    good_i3,
                    good_it 
                FROM    livedata_snapshotrun s
                JOIN    livedata_run r
                    ON s.run_id = r.id
                WHERE runNumber = $run_id 
                ORDER BY snapshot_id DESC 
                LIMIT 1";

        $result = array();

        $query = $this->live->query($sql);
        while($info = $query->fetch_assoc()) {
            $result[] = $info;
        }

        return $result;
    }

    private function get_gcd_files_from_convey($path, $run_id) {
        // Get folder listing
        $folder = file($this->datawarehouse_prefix . $path);

        $files = array();

        foreach($folder as &$line) {
            $matches = null;
            if(preg_match('/\<a\ href\=\"([_a-zA-Z0-9-\.]+Run[0]*' . $run_id . '[_a-zA-Z0-9-\.]+)\"\>/', $line, $matches)) {
                $files[] = ProcessingJobs::join_paths($path, $matches[1]);
            }
        }

        return $files;
    }

    private function get_gcd_files($run_id, $date_raw) {
        // Date format is YYYY-MM-DD
        $date = explode('-', $date_raw);

        if(count($date) != 3) {
            throw new InvalidArgumentException("Cannot search for GCD files since date format is not as expected: $date_raw");
        }

        $year = intval($date[0]);
        $month = intval($date[1]);
        $day = intval($date[2]);

        // Path depends on date
        $paths = array();
        if(($year == 2011 && $month <=5) || ($year < 2011 && $year >= 2007)) {
            $path[] = "/data/exp/IceCube/$year/filtered/level2a/{$date[1]}{$date[2]}/";
            $path[] = "/data/exp/IceCube/$year/filtered/level2/{$date[1]}{$date[2]}/";
        }

        if($year >= 2011) {
            $paths[] = "/data/exp/IceCube/$year/filtered/level2/AllGCD/";
        }

        $files = array();

        foreach($paths as $path) {
            $files = array_merge($files, $this->get_gcd_files_from_convey($path, $run_id));
        }

        return $files;
    }

    public function execute() {
        if(is_null($this->run_id) && !is_null($this->event_id)) {
            throw new InvalidArgumentException('Run id is required for event search');
        }

        if(is_null($this->run_id)) {
            throw new InvalidArgumentException('Run id is required');
        }

        $this->result['data']['result'] = array('successfully' => false,
                                                'message' => "Run {$this->run_id} was not found");

        $this->result['data']['query'] = array('run_id' => $this->run_id);

        if(!is_null($this->event_id)) {
            $this->result['data']['query']['event_id'] = $this->event_id;
        }

        $gr_info = $this->get_grl_info($this->run_id);

        if(count($gr_info) > 0) {
            $this->result['data']['result']['successfully'] = true;
            $this->result['data']['result']['good_it'] = (bool)($gr_info[0]['good_it']);
            $this->result['data']['result']['good_i3'] = (bool)($gr_info[0]['good_i3']);
            $this->result['data']['result']['date'] = $gr_info[0]['date'];
    
            $this->result['data']['result']['message'] = "Run {$this->run_id} was found.";

            // GCD files
            $gcd_files = $this->get_gcd_files($this->run_id, $gr_info[0]['date']);
            if(count($gcd_files) > 0) {
                $this->result['data']['result']['gcd_files'] = $gcd_files;
            }
        }

        // Search modes
        if(!is_null($this->run_id) && !is_null($this->event_id)) {
            // Search for event id
            $this->result['data']['query'] = array('run_id' => $this->run_id, 'event_id' => $this->event_id);

            $gaps_files = $this->get_gaps_files($this->run_id);
            $gaps_file = $this->get_subrun_by_event_id_and_gaps_files($gaps_files, $this->event_id);

            $this->result['data']['result']['successfully'] = false;
            $this->result['data']['result']['message'] = "Event {$this->event_id} was not found in run {$this->run_id}";

            if(false !== $gaps_file) {
                $this->result['data']['result']['successfully'] = true;

                $this->result['data']['result']['sub_run'] = intval(substr($gaps_file, -17, 8));
                $this->result['data']['result']['message'] = "Event {$this->event_id} was successfully found in sub run {$this->result['data']['result']['sub_run']} of run {$this->run_id}";

                $files = $this->get_files_for_subrun_and_run($this->run_id, $this->result['data']['result']['sub_run']);
                $this->result['data']['result']['files'] = $files;
            }
        } else {
            // Search for run id
            $this->result['data']['query'] = array('run_id' => $this->run_id);

            $paths = $this->get_paths_and_datasets($this->run_id);

            if(count($paths) > 0) {
                $this->result['data']['result']['successfully'] = true;
                $this->result['data']['result']['message'] = "Run {$this->run_id} was successfully found in " . count($paths) . " dataset" . (count($paths) > 1 ? 's' : '');
                $this->result['data']['result']['paths'] = $paths;
            }
        }

        return $this->result;
    }
}
