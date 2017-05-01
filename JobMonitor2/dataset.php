<?php

require('config.php');
require('./resources/class.Dataset.php');

header('Content-Type: application/json');

try {
    $dataset = new Dataset($CONFIG['db_host'],
                                $CONFIG['db_username'],
                                $CONFIG['db_password'],
                                $CONFIG['db_database'],
                                $CONFIG['data_warehouse'],
                                $CONFIG['api_version'],
                                $CONFIG['filter_db_host'],
                                $CONFIG['filter_db_username'],
                                $CONFIG['filter_db_password'],
                                $CONFIG['filter_db_database']);
    
    if(isset($_GET['dataset']) && trim($_GET['dataset']) !== '') {
        $dataset->set_dataset_id(filter_input(INPUT_GET, 'dataset'));
    }
   
    print(json_encode($dataset->execute()));
} catch(Exception $e) {
    $content = Dataset::$result_pattern;
    $content['api_version'] = $CONFIG['api_version'];
    $content['error'] = 1;
    $content['error_msg'] = $e->getMessage();
    $content['error_trace'] = $e->getTraceAsString();

    print(json_encode($content));
}
