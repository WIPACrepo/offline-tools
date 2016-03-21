<?php

require('config.php');
require('./resources/class.ProcessingJobs.php');

$pjobs = new ProcessingJobs($CONFIG['db_host'], $CONFIG['db_username'], $CONFIG['db_password'], $CONFIG['db_database']);

// Set defaults
$pjobs->set_dataset_id($CONFIG['default_dataset_id']);
$pjobs->set_completed_job_length($CONFIG['default_completed_job_length']);

if(isset($_GET['dataset_id'])) {
    $pjobs->set_dataset_id(filter_input(INPUT_GET, 'dataset_id'));
}

if(isset($_GET['completed_job_length'])) {
    $pjobs->set_completed_job_length(filter_input(INPUT_GET, 'completed_job_length'));
}

if(isset($_GET['options'])) {
    $options = explode(',', filter_input(INPUT_GET, 'options'));
    
    $pjobs->set_query_calendar(in_array('calendar', $options));
    $pjobs->set_query_current_jobs(in_array('current_jobs', $options));
    $pjobs->set_query_completed_jobs(in_array('completed_jobs', $options));
}

print(json_encode($pjobs->execute()));
