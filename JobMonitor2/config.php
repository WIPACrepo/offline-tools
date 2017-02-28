<?php

// Config

$CONFIG = array(
    'offline_processing_personnel' => array('name' => 'Jan Oertlin',
                                            'email' => 'jan.oertlin@icecube.wisc.edu',
                                            'slack' => array('user' => '@jan',
                                                             'channel' => '#data-processing')
                                           ),
    'db_username' => 'i3filter_ro',
    'db_password' => 'Z&F7?Hu"',
    'db_host' => 'dbs4',
    'db_database' => 'i3filter',
    'live_db_username' => 'icecube',
    'live_db_password' => 'skua',
    'live_db_host' => 'cygnus',
    'live_db_database' => 'live',
    'filter_db_username' => 'i3filter_read',
    'filter_db_password' => 'ce7f29816fd832',
    'filter_db_host' => 'filter-db',
    'filter_db_database' => 'i3filter',
    'default_dataset_id' => -1,
    'default_completed_job_length' => 10,
    'L2_datasets' => array(1863, 1866, 1870, 1871, 1874, 1883, 1888),
    'api_version' => '1.1',
    'svn' => '$Id$',
    'svn_url' => 'http://code.icecube.wisc.edu/projects/icecube/browser/IceCube/sandbox/jan/JobMonitor2',
    'data_warehouse' => 'http://icecube:skua@128.104.255.226',
    'path_prefixes' => array('file:', 'gsiftp://gridftp.icecube.wisc.edu'),
    '_version' => 1
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

require_once('./resources/class.Tools.php');

Tools::set_config($CONFIG);
