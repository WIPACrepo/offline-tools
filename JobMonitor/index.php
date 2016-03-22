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
        <script type="text/javascript" src="./js/js.cookie.js"></script>
        <script type="text/javascript" src="./js/jquery-ui/jquery-ui.min.js"></script>
        <script type="text/javascript" src="./js/jquery.dataTables.js"></script>
        <script type="text/javascript" src="./js/class.jobmonitor.js"></script>
        <script type="text/javascript" src="./js/jobmonitor.js"></script>
    </head>
    <body>
        <div id="header">
            <div class="wrapper" id="nav">
                <div id="error"></div>
                <div id="title">Offline Processing Job Monitor</div>
                <div id="last_update">Last Update: <span>never</span> <strong style="display: none;"></strong><img src="./images/loading.gif" style="display: none;" /></div>
                <div id="update_interval">
                    Update Interval:
                    <select></select>
                    <img src="./images/update.png" id="update_now" title="Update Now" />
                </div>
                <div id="dataset_id">
                    Dataset:
                    <select>
                    <?php
                    
                    foreach($dataset_list as $id) {
                        print("<option value=\"{$id['dataset_id']}\">{$id['dataset_id']}: {$id['description']}</option>\n");
                    }
                    
                    ?>
                    </select>
                </div>
                <div id="change_indicator" title="Data Changed Recently"></div>
            </div>
        </div>
        <div id="extended_data" title="Tails of log files"><b>Select Job ID:</b> <select></select><div id="tabs"></div></div>

        <div id="frame">
            <div class="toggle highlight">
                <h2 class="captain">Job Calendar:</h2>
                <div id="calendar" class="toggle-content"><div style="text-align: center;"><img src="./images/loading.gif" /></div></div>
            </div>

            <div class="toggle highlight">
                <h2 class="captain">Currently Processing Or Failed Jobs:</h2>
                <div id="current_jobs" class="jobtable toggle-content"></div>
            </div>

            <div class="toggle highlight">
                <h2><span class="captain">Recently Successfully Completed Jobs</span> (<select name="completed_job_length"><select>):</h2>
                <div id="completed_jobs" class="jobtable toggle-content"></div>
            </div>
        </div>

        <div class="wrapper" id="footer">
            Inofficial Offline Processing Job Monitor. Created by Jan Oertlin. If you've found bugs or you need another feature, write me an <a href="mailto:jan.oertlin@icecube.wisc.edu">email</a>.
        </div>
    </body>
</html>
