<?php

class Dataset {
    private $mysql;

    private $dataset_id;

    public static $result_pattern = array('api_version' => null,'error' => 0, 'error_msg' => '', 'error_trace' => '', 'data' => array());

    public function __construct($host, $user, $password, $db, $datawarehouse_prefix, $api_version) {
        $this->mysql = @new mysqli($host, $user, $password, $db);

        $this->result = self::$result_pattern;
        $this->result['api_version'] = $api_version;
        $this->dataset_id = null;
    }

    public function set_dataset_id($dataset) {
        $this->dataset_id = intval($dataset);
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
                                                    'jobs' => intval($jobs['num']));
        }
    }

    public function add_total_number_of_jobs() {
        $sql = "SELECT COUNT(*) AS `num` FROM i3filter.job WHERE dataset_id = {$this->dataset_id}";
        $fetch = $this->mysql->query($sql)->fetch_assoc();
        $this->result['data']['number_of_jobs'] = intval($fetch['num']);
    }

    private static function make_host($name) {
        $splitname = explode('.', $name);

        $host = array_slice($splitname, -2);

        if(preg_match("/^cn[0-9]+\.local$/", $name)) {
            return 'stanford.edu';
        #} elseif(substr($host[count($host) - 1], 0, 4) == 'tier') {
        #    return $host[count($host) - 1];
        } elseif(preg_match("/^n[0-9]{4}$/", $host[0])) {
            return 'hyak.washington.edu';
        } elseif(preg_match("/^cwrc\-c[0-9]{2}$/", $name)) {
            return 'westgrid.ca';
        } elseif(preg_match("/^muon-[0-9]{3}$/", $name)) {
            return 'eri.u-tokyo.ac.jp';
        } elseif(preg_match("/^cl[1-2]{1}n[0-9]{3}$/", $name)) {
            return 'westgrid.ca, ualberta.ca';
        } elseif(preg_match("/^[ajeigh][0-9]{4}$/", $name)) {
            return 'uni-mainz.de';
        } elseif(preg_match("/^gpc-f[0-9]{3}n[0-9]{3}-ib0$/", $name)) {
            return 'scinet.utoronto.ca';
        } elseif(preg_match("/^compute\-[0-9]{1,2}[n]{0,1}\-[0-9]{1,2}\.tier2$/", $name) || preg_match("/^blade\-[0-9]+\.tier2$/", $name)) {
            return 'ultralight.org';
        } elseif(strtolower($splitname[count($splitname) - 1]) == 'comet') {
            return 'COMET OSG VM';
        } elseif(count($splitname) > 3) {
            if($splitname[count($splitname) - 3] == 'icecube') {
                return implode('.', array_slice($splitname, -3));
            }
        }

        return implode('.', $host);
    }

    private static function std_square($x, $mean) {
        return pow($x - $mean, 2);
    }
    
    private static function std(array $array) {
        return sqrt(array_sum(array_map(array('Dataset', 'std_square'), $array, array_fill(0, count($array), (array_sum($array) / count($array))))) / (count($array)));
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
   
        $query = $this->mysql->query($sql);
        while($set = $query->fetch_assoc()) {
            $host = self::make_host($set['host']);
            if(!array_key_exists($host, $data)) {
                $data[$host] = array('i3exec runtime' => array());
            }

            $data[$host]['i3exec runtime'][] = $set['value'];
        }

        foreach($data as &$value) {
            $value['exec_average'] = array_sum($value['i3exec runtime']) / (double) count($value['i3exec runtime']);
            $value['exec_std'] = self::std($value['i3exec runtime']);
            $value['jobs'] = count($value['i3exec runtime']);
            unset($value['i3exec runtime']);
        }

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
        AND status IN ('OK' , 'FAILED')
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

    public function execute() {
        if(is_null($this->dataset_id)) {
            throw new Exception("No dataset given.");
        }

        $this->add_storage_information();   
        $this->add_metaproject_information();
        $this->add_grid_information();
        $this->add_total_number_of_jobs();
        $this->add_runntime_statistics();
        $this->add_num_of_jobs_completed_per_day();
    
        $this->result['data']['dataset_id'] = $this->dataset_id;

        return $this->result;
    }
}
