<?php

require('config.php');
require('./resources/class.ProcessingJobs.php');

$pjobs = new ProcessingJobs($CONFIG['db_host'], $CONFIG['db_username'], $CONFIG['db_password'], $CONFIG['db_database']);  

$dataset_list = $pjobs->get_dataset_ids();

?><!DOCTYPE html PUBLIC "-//W3C//DTD HTML+RDFa 1.1//EN">
<html lang="en" dir="ltr" version="HTML+RDFa 1.1"
  xmlns:content="http://purl.org/rss/1.0/modules/content/"
  xmlns:dc="http://purl.org/dc/terms/"
  xmlns:foaf="http://xmlns.com/foaf/0.1/"
  xmlns:og="http://ogp.me/ns#"
  xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
  xmlns:sioc="http://rdfs.org/sioc/ns#"
  xmlns:sioct="http://rdfs.org/sioc/types#"
  xmlns:skos="http://www.w3.org/2004/02/skos/core#"
  xmlns:xsd="http://www.w3.org/2001/XMLSchema#">
    <head profile="http://www.w3.org/1999/xhtml/vocab">
        <title>Offline Processing Job Monitor</title>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
        <link type="text/css" rel="stylesheet" href="./js/jquery-ui/jquery-ui.min.css" media="all" />
        <link type="text/css" rel="stylesheet" href="./css/jquery.dataTables.min.css" media="all" />
        <link type="text/css" rel="stylesheet" href="./css/style.css" media="all" />
        <script type="text/javascript" src="./js/jquery.min.js"></script>
        <script type="text/javascript" src="./js/jquery-ui/jquery-ui.min.js"></script>
        <script type="text/javascript" src="./js/jquery.dataTables.js"></script>
        <script type="text/javascript" src="./js/jobmonitor.js"></script>
    </head>
    <body>
        <div id="header">
            <div class="wrapper">
            <h1>Offline Processing Job Monitor</h1>
            </div>
            <div class="wrapper" id="nav">
            <div id="error"></div>
            <div id="last_update">Last Update: <span>never</span> <strong style="display: none;"></strong><img src="./images/loading.gif" style="display: none;" /></div>
            <div id="update_interval">
                Update Interval:
                <select></select>
                <img src="./images/update.png" id="update_now" />
            </div>
            <div id="dataset_id">
                Dataset:
                <select>
                <?php
                
                foreach($dataset_list as $id) {
                    $selected = '';
                    if($id['dataset_id'] == $CONFIG['default_dataset_id']) {
                        $selected = ' selected';
                    }

                    print("<option value=\"{$id['dataset_id']}\"$selected>{$id['dataset_id']}: {$id['description']}</option>\n");
                }
                
                ?>
                </select>
            </div>
            </div>
        </div>
        <div id="extended_data" title="Tails of log files"><b>Select Job ID:</b> <select></select><div id="tabs"></div></div>

        <div id="frame">
            <h2>Currently Processing Or Failed Jobs:</h2>
            <div id="current_jobs" class="jobtable"></div>

            <h2>Recently Successfully Completed Jobs (<select name="completed_job_length"><select>):</h2>
            <div id="completed_jobs" class="jobtable"></div>
        </div>
    </body>
<html>
