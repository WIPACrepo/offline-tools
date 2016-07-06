<?php

// Config

$CONFIG = array(
    'db_username' => 'i3filter_ro',
    'db_password' => 'Z&F7?Hu"',
    'db_host' => 'dbs4',
    'db_database' => 'i3filter',
    'default_dataset_id' => -1,
    'default_completed_job_length' => 10,
    'L2_datasets' => array(1863, 1866, 1870, 1871, 1874, 1883, 1888),
    'api_version' => '1.1',
    'svn' => '$Id$',
    'data_warehouse' => 'http://icecube:skua@128.104.255.226'
);

// Change svn value
function config_svn_parse(&$CONFIG) {
    $svn = array('date' => null, 'rev' => null, 'author' => null);
    $parts = explode(' ', $CONFIG['svn']);

    $svn['date'] = "{$parts[3]}";
    $svn['rev'] = $parts[2];
    $svn['author'] = $parts[5];

    $CONFIG['svn'] = $svn;
}

config_svn_parse($CONFIG);
