<?php

class TestRuns {
    private $filter_db;
    private $seasons;

    public function __construct($filter_db_host, $filter_db_user = null, $filter_db_password = null, $filter_db_db = null) {
        if($filter_db_host instanceof mysqli) {
            $this->filter_db = $filter_db_host;
        } else {
            $this->filter_db = @new mysqli($filter_db_host, $filter_db_user, $filter_db_password, $filter_db_db);
        }

        $this->seasons = null;
    }

    public function get_list() {
        if(is_null($this->seasons)) {
            $result = array();

            $sql = "SELECT * FROM i3filter.seasons ORDER BY season";
            $query = $this->filter_db->query($sql);
            while($set = $query->fetch_assoc()) {
                $result[$set['season']] = $set;
                $tr = array_map('intval', explode(',', $set['test_runs']));
                $result[$set['season']]['test_runs'] = $tr;
                $result[$set['season']]['first_run'] = intval($result[$set['season']]['first_run']);
            }

            $this->seasons = $result;
        }

        return $this->seasons;
    }

    public function get_season_by_run_id($run_id) {
        // Ensure that the list is loaded
        $this->get_list();

        $run_id = intval($run_id);
        $found_season = -1;

        foreach($this->seasons as $season => $v) {
            if(($run_id >= $v['first_run'] && $v['first_run'] != -1) || in_array($run_id, $v['test_runs'])) {
                $found_season = $season;
            }

            if($run_id < $v['first_run'] && $found_season > -1) {
                return $found_season;
            }
        }

        return $found_season;
    }
}
