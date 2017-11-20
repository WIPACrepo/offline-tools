<?php

#require_once('class.Timer.php');

if (!function_exists('stats_standard_deviation')) {
    /**
     * This user-land implementation follows the implementation quite strictly;
     * it does not attempt to improve the code or algorithm in any way. It will
     * raise a warning if you have fewer than 2 values in your array, just like
     * the extension does (although as an E_USER_WARNING, not E_WARNING).
     *
     * @param array $a
     * @param bool $sample [optional] Defaults to false
     * @return float|bool The standard deviation or false on error.
     */
    function stats_standard_deviation(array $a, $sample = false) {
        $n = count($a);
        if ($n === 0) {
            trigger_error("The array has zero elements", E_USER_WARNING);
            return false;
        }
        if ($sample && $n === 1) {
            trigger_error("The array has only 1 element", E_USER_WARNING);
            return false;
        }
        $mean = array_sum($a) / $n;
        $carry = 0.0;
        foreach ($a as $val) {
            $d = ((double) $val) - $mean;
            $carry += $d * $d;
        };
        if ($sample) {
           --$n;
        }
        return sqrt($carry / $n);
    }
}

class Dataset {
    private $mysql;
    private $dataset_id;
    private $source_dataset_id;
    private $result;
    private $filter_db;
    private $include_statistics;

    private $dataset_info;

    private static $node_regex = null;

    public static $result_pattern = array('api_version' => null,'error' => 0, 'error_msg' => '', 'error_trace' => '', 'data' => array());

    public function __construct($host, $user, $password, $db, $datawarehouse_prefix, $api_version, $filter_db_host, $filter_db_user, $filter_db_password, $filter_db_db) {
        $this->mysql = @new mysqli($host, $user, $password, $db);
        $this->filter_db = @new mysqli($filter_db_host, $filter_db_user, $filter_db_password, $filter_db_db);

        $this->result = self::$result_pattern;
        $this->result['api_version'] = $api_version;
        $this->dataset_id = null;
        $this->source_dataset_id = null;
        $this->include_statistics = false;
        $this->dataset_info = null;

        if(is_null(self::$node_regex)) {
            self::read_node_regex();
        }
    }

    public function set_include_statistics() {
        $this->include_statistics = true;
    }

    public function set_dataset_id($dataset) {
        $this->dataset_id = intval($dataset);
        $this->source_dataset_id = $this->get_parent_id($this->dataset_id);

        $this->dataset_info = $this->get_dataset_info();
    }

    private function get_dataset_info() {
        $sql = "SELECT * FROM i3filter.datasets WHERE dataset_id = {$this->dataset_id}";
        $query = $this->filter_db->query($sql);

        if($query->num_rows != 1) {
            throw new Exception('Cannot query dataset information');
        }

        $data = $query->fetch_assoc();

        if($data['type'] == 'L3' || $data['type'] == 'L4') {
            $sql = "SELECT * FROM i3filter.level3_config WHERE dataset_id = {$this->dataset_id}";
            $query = $this->filter_db->query($sql);

            if($query->num_rows == 1) {
                $data['__level3'] = $query->fetch_assoc();
            } else {
                 $data['__level3'] = array();
            }
        }

        return $data;
    }

    private function add_parents() {
        $this->result['data']['source_dataset_ids'] = $this->source_dataset_id;
    }

    private function get_parent_id($dataset_id) {
        $sql = "SELECT * FROM i3filter.source_dataset_id WHERE dataset_id = {$dataset_id}";

        $query = $this->filter_db->query($sql);
        $parents = array();
        while($row = $query->fetch_assoc()) {
            $parents[] = $row['source_dataset_id'];
        }

        return $parents;
    }

    private function add_level3_information() {
        if($this->dataset_info['type'] == 'L3' || $this->dataset_info['type'] == 'L4') {
            $this->result['data']['level3_information'] = $this->dataset_info['__level3'];
            if(isset($this->result['data']['level3_information']['dataset_id'])) {
                unset($this->result['data']['level3_information']['dataset_id']);
            }
        }
    }

    private function add_metaproject_information() {
        $sql = "SELECT 
    *
FROM
    i3filter.metaproject mp
        JOIN
    i3filter.metaproject_pivot mpp ON mp.metaproject_id = mpp.metaproject_id
        JOIN
    i3filter.metaproject_tarball mpt ON mp.metaproject_id = mpt.metaproject_id
WHERE
    dataset_id = {$this->dataset_id}";

        $query = $this->mysql->query($sql);

        while($fetch = $query->fetch_assoc()) {
            $this->result['data']['metaproject'] = $fetch['versiontxt'];
            if(!isset($this->result['data']['tarball'])) {
                $this->result['data']['tarball'] = array();
            }

            $this->result['data']['tarball'][] = basename($fetch['relpath']);
        }
    }

    private function add_storage_information() {
        $sql = "SELECT SUM(size) AS `size`, COUNT(*) AS `num` FROM i3filter.urlpath WHERE dataset_id = {$this->dataset_id} AND type = 'PERMANENT'";
        $query = $this->mysql->query($sql);
        $fetch = $query->fetch_assoc();
        $this->result['data']['output'] = array('size' => intval($fetch['size']), 'files' => intval($fetch['num']));

        $sql = "SELECT SUM(size) AS `size`, COUNT(*) AS `num` FROM i3filter.urlpath WHERE dataset_id = {$this->dataset_id} AND type = 'INPUT'";
        $query = $this->mysql->query($sql);
        $fetch = $query->fetch_assoc();
        $this->result['data']['input'] = array('size' => intval($fetch['size']), 'files' => intval($fetch['num']));
    }

    private function add_grid_information() {
        $sql = "SELECT * FROM i3filter.grid g JOIN i3filter.grid_statistics gs ON g.grid_id = gs.grid_id WHERE dataset_id = {$this->dataset_id}";

        $query = $this->mysql->query($sql);

        $this->result['data']['grid'] = array();

        while($fetch = $query->fetch_assoc()) {
            $sql = "SELECT COUNT(*) AS `num` FROM i3filter.job WHERE dataset_id = {$this->dataset_id} AND grid_id = " . intval($fetch['grid_id']);
            $jobs = $this->mysql->query($sql)->fetch_assoc();

            $this->result['data']['grid'][] = array('name' => $fetch['name'],
                                                    'failures' => intval($fetch['failures']),
                                                    'evictions' => intval($fetch['evictions']),
                                                    'jobs' => intval($jobs['num']),
                                                    'enabled' => intval($fetch['suspend']) == 0);
        }
    }

    public function add_total_number_of_jobs() {
        $sql = "SELECT COUNT(*) AS `num` FROM i3filter.job WHERE dataset_id = {$this->dataset_id}";
        $fetch = $this->mysql->query($sql)->fetch_assoc();
        $this->result['data']['number_of_jobs'] = intval($fetch['num']);
    }

    private static function read_node_regex() {
        self::$node_regex = array();

        $file = file('node-names.txt');

        // Skip first line
        for($i = 1; $i < count($file); ++$i) {
            if(strlen(trim($file[$i])) == 0) {
                // Skip empty line
                continue;
            }

            $data = array_map(trim, explode(' ', $file[$i], 2));

            self::$node_regex[] = array('regex' => $data[0], 'name' => $data[1]);
        }
    }

    private static function make_host($name) {
        foreach(self::$node_regex as $data) {
            if(preg_match($data['regex'], $name)) {
                return $data['name'];
            }
        }

        $splitname = explode('.', $name);
        $host = array_slice($splitname, -2);

        return implode('.', $host);
    }

    private static function std(array $array) {
        return stats_standard_deviation($array);
    }

    public function add_runntime_statistics() {
        $sql = "SELECT 
    `host`, `name`, `value`
FROM
    i3filter.job j
        JOIN
    i3filter.job_statistics s ON s.dataset_id = j.dataset_id
        AND s.queue_id = j.queue_id
WHERE
    `host` IS NOT NULL AND name = 'i3exec runtime' AND j.dataset_id = {$this->dataset_id}";
     
        $data = array();
  
#        $t = new Timer();

#        $t->start();
 
        $query = $this->mysql->query($sql);
        while($set = $query->fetch_assoc()) {
            $host = self::make_host($set['host']);
            if(!array_key_exists($host, $data)) {
                $data[$host] = array('i3exec runtime' => array());
            }

            $data[$host]['i3exec runtime'][] = $set['value'];
        }

#        $t->stop()->print_elapsed('Runntime Statistics: while')->start();

        foreach($data as &$value) {
            $value['exec_average'] = array_sum($value['i3exec runtime']) / (double) count($value['i3exec runtime']);
            $value['exec_std'] = self::std($value['i3exec runtime']);
            $value['jobs'] = count($value['i3exec runtime']);
            unset($value['i3exec runtime']);
        }

#        $t->stop()->print_elapsed('Runntime Statistics: foreach');

        $this->result['data']['statistics']['execution_time'] = $data;
    }

    public function add_num_of_jobs_completed_per_day() {
        $this->result['data']['statistics']['job_completion'] = array();

        $sql = "SELECT 
    DATE(status_changed) AS `date`,
    g.name AS `grid_name`,
    COUNT(*) AS `jobs`
FROM
    i3filter.job j
        JOIN
    i3filter.grid g ON j.grid_id = g.grid_id
WHERE
    dataset_id = {$this->dataset_id}
        AND status = 'OK'
        AND status_changed IS NOT NULL
GROUP BY DATE(status_changed) , j.grid_id";

        $query = $this->mysql->query($sql);

        while($row = $query->fetch_assoc()) {
            if(!array_key_exists($row['date'], $this->result['data']['statistics']['job_completion'])) {
                $this->result['data']['statistics']['job_completion'][$row['date']] = array();
            }

            $this->result['data']['statistics']['job_completion'][$row['date']][] = array('grid' => $row['grid_name'], 'jobs' => intval($row['jobs']));
        }
    }

    private function get_run_completion_time($dataset_id) {
        $sql = "SELECT 
    run_id, MAX(UNIX_TIMESTAMP(status_changed)) AS `date`
FROM
    i3filter.job j
        JOIN
    i3filter.run r ON j.dataset_id = r.dataset_id
        AND j.queue_id = r.queue_id
WHERE
    j.dataset_id = {$dataset_id}
        AND status_changed IS NOT NULL
GROUP BY run_id";

        $query = $this->mysql->query($sql);
        $result = array();

        while($row = $query->fetch_assoc()) {
            $result["{$row['run_id']}"] = $row;
        }

        return $result;
    }

    public function add_parent_delay() {
        if(!is_null($this->source_dataset_id)) {
            $parents = array();
            foreach($this->source_dataset_id as $source_dataset_id) {
                $parents = $parents + $this->get_run_completion_time($source_dataset_id);
            }

            $current = $this->get_run_completion_time($this->dataset_id);

            $data = array();

            foreach($current as $run_id => $d) {
                if(array_key_exists($run_id, $parents)) {
                    $data[$run_id] = $d['date'] - $parents[$run_id]['date'];
                }
            }

             $this->result['data']['statistics']['source_dataset_completion_delay'] = $data;
        }
    }

    public function execute() {
        if(is_null($this->dataset_id)) {
            throw new Exception("No dataset given.");
        }

#        $t = new Timer();
#        $t->start();
        $this->add_storage_information();   
#        $t->stop()->print_elapsed('Storage Information');

#        $t->start();
        $this->add_metaproject_information();
#        $t->stop()->print_elapsed('metaproject Information');

#        $t->start();
        $this->add_grid_information();
#        $t->stop()->print_elapsed('Grid Infrormation');

#        $t->start();
        $this->add_total_number_of_jobs();
#        $t->stop()->print_elapsed('Total Number of Jobs');

#        $t->start();
        $this->add_level3_information();
#        $t->stop()->print_elapsed('Level3 Information');

        if($this->include_statistics) {
#            $t->start();
            $this->add_runntime_statistics();
#            $t->stop()->print_elapsed('Runntime Statistics');

#            $t->start();
            $this->add_num_of_jobs_completed_per_day();
#            $t->stop()->print_elapsed('Job Completion/Day');

#            $t->start();
            $this->add_parent_delay();
#            $t->stop()->print_elapsed('Parent Delay');
        }

        $this->add_parents();

        $this->result['data']['dataset_id'] = $this->dataset_id;

        return $this->result;
    }
}
