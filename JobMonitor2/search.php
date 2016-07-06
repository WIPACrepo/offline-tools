<?php

require('config.php');
require('./resources/class.Search.php');

header('Content-Type: application/json');

try {
    $search = new Search($CONFIG['db_host'],
                                $CONFIG['db_username'],
                                $CONFIG['db_password'],
                                $CONFIG['db_database'],
                                $CONFIG['data_warehouse'],
                                $CONFIG['api_version']);
    
    if(isset($_GET['run_id']) && trim($_GET['run_id']) !== '') {
        $search->set_run_id(filter_input(INPUT_GET, 'run_id'));
    }
   
    if(isset($_GET['event_id']) && trim($_GET['event_id']) !== '') {
        $search->set_event_id(filter_input(INPUT_GET, 'event_id'));
    }

    print(json_encode($search->execute()));
} catch(Exception $e) {
    $content = Search::$result_pattern;
    $content['api_version'] = $CONFIG['api_version'];
    $content['error'] = 1;
    $content['error_msg'] = $e->getMessage();
    $content['error_trace'] = $e->getTraceAsString();

    print(json_encode($content));
}
