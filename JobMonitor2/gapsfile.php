<?php

require('config.php');

header('Content-Type: application/json');

function get_data($filter_db_host, $filter_db_user, $filter_db_password, $filter_db_db, $run_id, $pass2) {
    $db = @new mysqli($filter_db_host, $filter_db_user, $filter_db_password, $filter_db_db);

    $table = $pass2 ? 'i3filter.sub_runs_pass2' : 'i3filter.sub_runs';

    $sql = "SELECT 
         `sub_run` AS `file_number`,
        `first_event`,
        `last_event`,
        `first_event_year`,
        `first_event_frac`,
        `last_event_year`,
        `last_event_frac`,
        `livetime`
    FROM {$table} WHERE run_id = {$run_id} ORDER BY `sub_run`";

    $data = array();

    $query = $db->query($sql);
    while($row = $query->fetch_assoc()) {
        if(array_key_exists('bad', $row)) {
            if(intval($row['bad'])) {
                continue;
            }
        }

        $data[] = $row;
    }

    return $data;
}

try {
    $content = array('error' => 0, 'error_msg' => '', 'data' => array());

    $pass2 = false;
    $run_id = 0;
    
    if(!isset($_GET['run_id'])) {
        throw new Exception('Missing run_id');
    }

    $run_id = filter_input(INPUT_GET, 'run_id', FILTER_VALIDATE_INT);

    if(false === $run_id || is_null($run_id) || $run_id <= 0) {
        throw new Exception('Invalid run_id value.');
    }

    if(isset($_GET['pass2'])) {
        $pass2 = filter_input(INPUT_GET, 'pass2', FILTER_VALIDATE_BOOLEAN);
    }

    $data = array($run_id => get_data($CONFIG['filter_db_host'], $CONFIG['filter_db_username'], $CONFIG['filter_db_password'], $CONFIG['filter_db_database'], $run_id, $pass2));

    $content['data'] = $data;

    print(json_encode($content));
} catch(Exception $e) {
    $content = array('error' => 0, 'error_msg' => '', 'data' => array());
    $content['error'] = 1;
    $content['error_msg'] = $e->getMessage();
    $content['error_trace'] = $e->getTraceAsString();

    print(json_encode($content));
}
