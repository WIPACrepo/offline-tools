<?php

// Debug?
//error_reporting(-1);
//ini_set('display_errors', 1);

// Config

$CONFIG = array(
    'offline_processing_personnel' => array('name' => 'Jan Oertlin',
                                            'email' => 'jan.oertlin@icecube.wisc.edu',
                                            'slack' => array('user' => '@jan',
                                                             'channel' => '#data-processing')
                                           ),
    'db_username' => 'i3filter_ro',
    'db_password' => 'Z&F7?Hu"',
    'db_host' => 'dbs4.icecube.wisc.edu',
    #'db_username' => 'i3filter',
    #'db_host' => 'juancarlosmysql.icecube.wisc.edu',
    'db_database' => 'i3filter',
    'live_db_username' => 'icecube',
    'live_db_password' => 'skua',
    'live_db_host' => 'cygnus.icecube.wisc.edu',
    'live_db_database' => 'live',
    'filter_db_username' => 'i3filter_read',
    'filter_db_password' => 'ce7f29816fd832',
    'filter_db_host' => 'filter-db.icecube.wisc.edu',
    #'filter_db_password' => '0a6f869d0c8fcc',
    #'filter_db_username' => 'i3filter',
    #'filter_db_host' => 'juancarlosmysql.icecube.wisc.edu',
    'filter_db_database' => 'i3filter',
    'default_dataset_id' => -1,
    'iceprod_token' => "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6ImRzY2h1bHR6Iiwicm9sZSI6InN5c3RlbSIsImdyb3VwcyI6W10sImlzcyI6IkljZVByb2QiLCJzdWIiOiJkc2NodWx0eiIsImV4cCI6MTU5NjY0Mzk2MC4wMTEyNzc3LCJuYmYiOjE1ODIwNjIxMDcuMDExMjc3NywiaWF0IjoxNTgyMDYyMTA3LjAxMTI3NzcsInR5cGUiOiJzeXN0ZW0ifQ.5Gc89jrZ9q0mkV5RPX5yOMDMjDpP1_3UwIqJlFNniOeJcJDUJK7hYrQtg33_09sPwJAAEAxRHTNICvZuwGcfEA",
    'default_completed_job_length' => 10,
    'api_version' => '1.2',
    'svn' => '$Id$',
    'svn_url' => 'http://code.icecube.wisc.edu/svn/sandbox/jan/JobMonitor2',
    'data_warehouse' => 'http://icecube:skua@convey.icecube.wisc.edu',
    'path_prefixes' => array('file:', 'gsiftp://gridftp.icecube.wisc.edu', 'http://convey.icecube.wisc.edu'),
    '_version' => 11
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

require_once('resources/class.Tools.php');

Tools::set_config($CONFIG);
