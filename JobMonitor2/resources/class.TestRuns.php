<?php

class TestRuns {
    private $filter_db;

    public function __construct($filter_db_host, $filter_db_user, $filter_db_password, $filter_db_db) {
        $this->filter_db = @new mysqli($filter_db_host, $filter_db_user, $filter_db_password, $filter_db_db);
    }

    public function get_list() {
        $result = array();

        $sql = "SELECT * FROM i3filter.seasons ORDER BY season";
        $query = $this->filter_db->query($sql);
        while($set = $query->fetch_assoc()) {
            $result[$set['season']] = $set;
            $tr = array_map('intval', explode(',', $set['test_runs']));
            $result[$set['season']]['test_runs'] = $tr;
            $result[$set['season']]['first_run'] = intval($result[$set['season']]['first_run']);
        }

        return $result;
    }
}
