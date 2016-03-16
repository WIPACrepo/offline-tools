<?php

require('config.php');
require('./resources/class.ProcessingJobs.php');

$pjobs = new ProcessingJobs($CONFIG['db_host'], $CONFIG['db_username'], $CONFIG['db_password'], $CONFIG['db_database']);

// Set defaults
$pjobs->set_dataset_id($CONFIG['default_dataset_id']);
$pjobs->set_completed_job_length($CONFIG['default_completed_job_length']);

if(isset($_GET['dataset_id'])) {
    $pjobs->set_dataset_id($_GET['dataset_id']);
}

if(isset($_GET['completed_job_length'])) {
    $pjobs->set_completed_job_length($_GET['completed_job_length']);
}

print(json_encode($pjobs->execute()));
