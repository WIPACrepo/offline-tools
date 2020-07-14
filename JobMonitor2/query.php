<?php

require('config.php');
require('./resources/class.ProcessingJobs.php');

header('Content-Type: application/json');

try {
    $pjobs = new ProcessingJobs($CONFIG['db_host'],
                                $CONFIG['db_username'],
                                $CONFIG['db_password'],
                                $CONFIG['db_database'],
                                $CONFIG['default_dataset_id'],
                                $CONFIG['api_version'],
                                $CONFIG['live_db_host'],
                                $CONFIG['live_db_username'],
                                $CONFIG['live_db_password'],
                                $CONFIG['live_db_database'],
                                $CONFIG['filter_db_host'],
                                $CONFIG['filter_db_username'],
                                $CONFIG['filter_db_password'],
                                $CONFIG['filter_db_database']);
    
    // Set defaults
    $pjobs->set_dataset_id($CONFIG['default_dataset_id']);
    
    if(isset($_GET['dataset_id'])) {
        $pjobs->set_dataset_id(filter_input(INPUT_GET, 'dataset_id'));
    }
   
    if(isset($_GET['dataset_list_only']) || !isset($_GET['dataset_id'])) {
        if(filter_input(INPUT_GET, 'dataset_list_only') || !isset($_GET['dataset_id'])) {
            $pjobs->set_dataset_list_only(true);
        }
    }

    print(json_encode($pjobs->execute()));
} catch(Exception $e) {
    $content = array('error' => 0, 'error_msg' => '', 'data' => array());
    $content['error'] = 1;
    $content['error_msg'] = $e->getMessage();
    $content['error_trace'] = $e->getTraceAsString();

    print(json_encode($content));
}

?>
