<?php

require_once('config.php');

?><!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Offline Processing JobMonitor 2</title>

    <!-- Bootstrap -->
    <link href="css/bootstrap.min.css" rel="stylesheet">
    <link href="css/bootstrap-select.min.css" rel="stylesheet">
    <link href="css/dataTables.bootstrap.min.css" rel="stylesheet">
    <link href="css/font-awesome.min.css" rel="stylesheet">
    <link href="css/awesome-bootstrap-checkbox.css" rel="stylesheet">
    <link href="css/jobmonitor2.css" rel="stylesheet">

    <!-- HTML5 shim and Respond.js for IE8 support of HTML5 elements and media queries -->
    <!-- WARNING: Respond.js doesn't work if you view the page via file:// -->
    <!--[if lt IE 9]>
      <script src="https://oss.maxcdn.com/html5shiv/3.7.2/html5shiv.min.js"></script>
      <script src="https://oss.maxcdn.com/respond/1.4.2/respond.min.js"></script>
    <![endif]-->

    <script src="js/jquery.min.js"></script>
    <script src="js/jquery.dataTables.min.js"></script>
    <script src="js/bootstrap.min.js"></script>
    <script src="js/bootstrap-select.min.js"></script>
    <script src="js/dataTables.bootstrap.min.js"></script>
    <script src="js/class.jobmonitor.location.js"></script>
    <script src="js/class.jobmonitor.view.js"></script>
    <script src="js/class.jobmonitor.views.js"></script>
    <script src="js/class.jobmonitor.calendar.js"></script>
    <script src="js/class.jobmonitor.jobs.js"></script>
    <script src="js/class.jobmonitor.updater.js"></script>
    <script src="js/class.jobmonitor.datasets.js"></script>
    <script src="js/class.jobmonitor.search.js"></script>
    <script src="js/class.jobmonitor.js"></script>
    <script src="js/jobmonitor.js"></script>
  </head>
  <body>
    <div class="jm-invisible" id="jm-api-version"><?php print($CONFIG['api_version']); ?></div>
    <div class="jm-invisible" id="jm-personnel" data-jm-name="<?php print($CONFIG['offline_processing_personnel']['name']); ?>" data-jm-email="<?php print($CONFIG['offline_processing_personnel']['email']); ?>" data-jm-slack-user="<?php print($CONFIG['offline_processing_personnel']['slack']['user']); ?>" data-jm-slack-channel="<?php print($CONFIG['offline_processing_personnel']['slack']['channel']); ?>"></div>
    <nav class="navbar navbar-default navbar-fixed-top">
      <div class="container-fluid">
        <!-- Brand and toggle get grouped for better mobile display -->
        <div class="navbar-header">
          <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#bs-example-navbar-collapse-1" aria-expanded="false">
            <span class="sr-only">Toggle navigation</span>
            <span class="icon-bar"></span>
            <span class="icon-bar"></span>
            <span class="icon-bar"></span>
          </button>
          <a class="navbar-brand" href="#">OPJM2</a>
        </div>
    
        <!-- Collect the nav links, forms, and other content for toggling -->
        <div class="collapse navbar-collapse" id="bs-example-navbar-collapse-1">
          <ul class="nav navbar-nav">
            <p class="navbar-text"><b>Last Update:</b> <span id="last-update-view">never</span></p>
            <li class="dropdown" id="update-interval-selection">
              <a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-haspopup="true" aria-expanded="false"><b>Update Interval:</b> <span id="current-interval">never</span> <span class="caret"></span></a>
              <ul class="dropdown-menu">
              </ul>
            </li>
            <li><a href="#" id="force-update"><i class="fa fa-refresh fa-lg fa-fw margin-bottom"></i></a></li>
          </ul>
          <ul class="nav navbar-nav navbar-right">
            <li><a href="#" data-toggle="modal" data-target="#jm-dialog-search">Search <span class="glyphicon glyphicon-search" aria-hidden="true"></span></a></li>
            <li class="dropdown" id="jm-view-dropdown">
              <a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-haspopup="true" aria-expanded="false">View <span class="caret"></span></a>
              <ul class="dropdown-menu">
                <li><a href="#" data-target="calendarView"><span class="glyphicon glyphicon-check" aria-hidden="true"></span> Calendar</a></li>
                <li><a href="#" data-target="jobsView"><span class="glyphicon glyphicon-check" aria-hidden="true"></span> Processing/Failed Jobs</a></li>
              </ul>
            </li>
            <li class="dropdown" id="jm-dataset-dropdown">
              <a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-haspopup="true" aria-expanded="false"><b>Dataset:</b> <span id="current-dataset">not selected</span> <span class="caret"></span></a>
              <ul class="dropdown-menu">
              </ul>
            </li>
            <li>
              <a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-haspopup="true" aria-expanded="false"><span class="glyphicon glyphicon-question-sign"></span></a>
              <ul class="dropdown-menu">
                <li><a href="#" data-toggle="modal" data-target="#jm-dialog-feedback">Feedback & Help</a></li>
                <li><a href="#" data-toggle="modal" data-target="#jm-dialog-api">API</a></li>
                <li role="separator" class="divider"></li>
                <li><a href="#" data-toggle="modal" data-target="#jm-dialog-version" target="_blank">Rev <?php print($CONFIG['svn']['rev']); ?> (<?php print($CONFIG['svn']['date']); ?>)</a></li>
              </ul>
            </li>
          </ul>
        </div><!-- /.navbar-collapse -->
      </div><!-- /.container-fluid -->
    </nav>

    <div id="content-frame">
      <div id="jm-dataset-title" style="margin-bottom: 20px"></div>

      <div class="alert alert-warning" role="alert" id="jm-l3-pre-2015-season-note" style="display: none;">
        <strong>Note:</strong> You are watching a L3 dataset before season 2015. For those runs no validation flag exists. Therefore, all runs are maked as validated by default.
      </div>

      <div class="alert alert-warning" role="alert" id="jm-l2-2010-2012-season-note" style="display: none;">
        <strong>Note:</strong> You are watching a L2 dataset for that no validation information is available. Therefore, all runs are maked as validated by default.
      </div>

      <div class="alert alert-danger" role="alert" id="jm-loading-error" style="display: none;">
        <strong>Error:</strong> An error occurred while loading data.
      </div>

      <div class="alert alert-danger" role="alert" id="jm-loading-error-customized" style="display: none;">
        <strong>Error:</strong> <span></span>
      </div>

      <div class="panel panel-default" id="jm-content-view-calendar" style="display: none">
        <div class="panel-heading">
          <h3 class="panel-title jm-view-header">Calendar View</h3>
        </div>
        <div class="panel-body jm-view-content">
          Not loaded yet...
        </div>
      </div>

      <div class="panel panel-default" id="jm-content-view-current-jobs" style="display: none">
        <div class="panel-heading">
          <h3 class="panel-title jm-view-header">Jobs</h3>
        </div>
        <div class="panel-body jm-view-content">
          Not loaded yet...
        </div>
      </div>    

      <div class="panel panel-default" id="jm-content-view-dataset-selection">
        <div class="panel-heading">
          <h3 class="panel-title jm-view-header">Select Dataset</h3>
        </div>
        <div class="panel-body jm-view-content">
          Not loaded yet...
        </div>
      </div>    
    </div>

    <div class="modal fade" id="jm-dialog-search" tabindex="-1" role="dialog" aria-labelledby="jm-dialog-label">
      <div class="modal-dialog" role="document">
        <div class="modal-content">
          <div class="modal-header">
            <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
            <h4 class="modal-title">Search <span class="glyphicon glyphicon-search" aria-hidden="true"></span></h4>
          </div>
          <div class="modal-body">
            <div class="input-group">
              <span class="input-group-addon" id="basic-addon3">Run-Id</span>
              <input type="text" class="form-control" id="jm-search-run-id" aria-describedby="basic-addon3">
            </div>

            <div class="checkbox">
              <input type="checkbox" data-dest="completed" id="jm-search-event-id-switch">
              <label for="jm-search-event-id-switch">Search for an Event</label>
            </div>

            <div class="input-group" id="jm-search-event-id-wrapper" style="display: none;">
              <span class="input-group-addon" id="basic-addon3">Event-Id</span>
              <input type="text" class="form-control" id="jm-search-event-id" aria-describedby="basic-addon3">
            </div>

            <div id="jm-search-buttonbar">
              <i class="fa fa-circle-o-notch fa-spin fa-2x fa-fw" id="jm-searching-indicator"></i><button type="button" class="btn btn-primary">Search</button>
            </div>

            <div id="jm-search-result-box">

            </div>
          </div>
        </div>
      </div>
    </div>

    <div class="modal fade" id="jm-dialog-feedback" tabindex="-1" role="dialog" aria-labelledby="jm-dialog-label">
      <div class="modal-dialog" role="document">
        <div class="modal-content">
          <div class="modal-header">
            <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
            <h4 class="modal-title">Feedback</h4>
          </div>
          <div class="modal-body">
            <ul>
              <li>Did you find some errors?</li>
              <li>Do you have any suggestions?</li>
              <li>You couldn't find what you were looking for?</li>
            </ul>

            <p>Write me an <a href="mailto:<?php print($CONFIG['offline_processing_personnel']['email']); ?>">email</a> or on Slack in <a href="https://icecube-spno.slack.com/messages/<?php print(substr($CONFIG['offline_processing_personnel']['slack']['channel'], 1)); ?>/" target="_blank"><?php print($CONFIG['offline_processing_personnel']['slack']['channel']); ?></a> or <a href="https://icecube-spno.slack.com/messages/<?php print($CONFIG['offline_processing_personnel']['slack']['user']); ?>/"><?php print($CONFIG['offline_processing_personnel']['slack']['user']); ?></a>.</p>
          </div>
        </div>
      </div>
    </div>

    <div class="modal fade" id="jm-dialog-version" tabindex="-1" role="dialog" aria-labelledby="jm-dialog-label">
      <div class="modal-dialog" role="document">
        <div class="modal-content">
          <div class="modal-header">
            <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
            <h4 class="modal-title">Version Information</h4>
          </div>
          <div class="modal-body">
            <p>
              The current revision is <?php print($CONFIG['svn']['rev']); ?> (<?php print($CONFIG['svn']['date']); ?>)
            </p>
            <p>
              API version: <?php print($CONFIG['api_version']); ?>
            </p>
            <p>
              Repository: <a href="<?php print($CONFIG['svn_url']); ?>" target="_blank">JobMonitor2</a>
            </p>
          </div>
        </div>
      </div>
    </div>

    <div class="modal fade" id="jm-dialog-api" tabindex="-1" role="dialog" aria-labelledby="jm-dialog-label">
      <div class="modal-dialog modal-lg" role="document">
        <div class="modal-content">
          <div class="modal-header">
            <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
            <h4 class="modal-title">API</h4>
          </div>
          <div class="modal-body">
            <p>
              This service uses a simple web interface to query all the information. You can use it as well!
            </p>
            <p>
              <?php $url = 'http://' . $_SERVER['SERVER_NAME'] . dirname($_SERVER['PHP_SELF']) . '/query.php'; ?>
              The interface is based on JSON and can be easily used within a Python script. The URL is: <a href="<?php print($url); ?>" target="_blank"><?php print($url); ?></a>
            </p>

            <h2>API Version</h2>
            <p>
              The basic meta data contains the api version. This is important for your client to check if it still supports the current API:
            </p>
<pre>{
    "api_version": "1.0"
}</pre>
            <p>
              The major version number indicates that something has been renamed, removed, or the data value has changed. Therefore, the major version number should be always the same!
            </p>
            <p>
              The minor version number indicates that something has been added to the API. Therefore, you can still use the API if <code>client_major == data_major &amp;&amp; client_minor &lt;= data_minor</code>.
            </p>
            <p>
              Any other meta data can change with the api version. Therefore, the following descriptions are separated by API versions.
            </p>

            <h2>Version 1.0</h2>
            <p>
              This is the very first API version.
            </p>

            <h3>Parameter</h3>
            <p>Description of the options.</p>
            <ul>
              <li>
                <code>dataset_id</code>:
                <p>
                  It specifies the dataset for that the run list is returned.
                </p>
                <div class="alert alert-info" role="alert">
                  <strong>Example:</strong> <code>http://.../query.php?dataset_id=1883</code>
                </div>    
                <div class="alert alert-info" role="alert">
                  <strong>Note:</strong> A dataset list is everytime returned. For the selected dataset is the value of <code>selected</code> <code>true</code>.
                </div>    
              </li>       
              <li>        
                <code>dataset_list_only</code>:
                <p>
                  Can be <code>true</code> or <code>false</code> (default if not specified). If you set it to <code>true</code>, only the list of datasets and seasons will be returned.
                  The list of runs will be empty.
                  <div class="alert alert-info" role="alert">
                    <strong>Example:</strong> <code>http://.../query.php?dataset_list_only=true</code>
                  </div>
                </p>
              </li>
            </ul>

            <h3>Response</h3>
            <p>Description of the server response.</p>

            <h4>Basic Structure</h4>
            <p>
              The basic structure looks like this:
<pre>{
    "api_version": 1,
    "error": 0,
    "error_msg": "",
    "data": {...}
}
</pre>
              The meta data is at the first level, the data is stored in the key <code>data</code>.
            </p>

            <h4>Meta Data</h4>
            <p>
              The meta data contains beside the <code>api_version</code> the following two keys:
              <ul>
                <li><code>error</code>: Can be <code>0</code> or <code>1</code>. If it is <code>0</code>, no error occurred and the response should be valid. If <code>1</code> or
                  something else is returned, something went wrong and you should not use the data result. <code>error_msg</code> may help to understand what went wrong.</li>
                <li><code>error_msg</code>: May help to understand what went wrong if <code>error</code> is not <code>0</code>.</li>
              </ul>
            </p>

            <h4>Data</h4>
            <p>
              The data is separated into three sub categories:
<pre>"data": {
    "runs": {...},
    "datasets": {...},
    "seasons": {...}
}</pre>
              The category <code>runs</code> contains all information about the <i>good</i> runs of the selected dataset; <code>datasets</code> holds all information about datasets and it is connected to <code>seasons</code>, which gives information about each season.
            </p>

            <div class="alert alert-info" role="alert"><strong>Note:</strong> It may not provide all available datasets and seasons. Some of the listed datasets my not be supported for <code>run</code> data.</div>

            <p>
              The following shows the structure of each category. In general, all categories are objects and the attribute names are the run id, dataset id and year of season.
            </p>
           <ul>
             <li>
               <code>runs</code>
               <p>The data structure is shown for an exampe:</p>
<pre>"124752": {
    "run_id": "124752",
    "sub_runs": "292",
    "date": "2014-05-15",
    "status": {
        "value": 5,
        "name": "FAILED"
    },
    "jobs_states": {
        "WAITING": "0",
        "QUEUEING": "0",
        "QUEUED": "0",
        "PROCESSING": "0",
        "OK": "290",
        "ERROR": "0",
        "READYTOCOPY": "0",
        "COPYING": "0",
        "SUSPENDED": "0",
        "RESET": "0",
        "FAILED": "2",
        "COPIED": "0",
        "EVICTED": "0",
        "CLEANING": "0",
        "IDLE": "0",
        "IDLEBDList": "0",
        "IDLEIncompleteFiles": "0",
        "IDLENoFiles": "0",
        "IDLETestRun": "0",
        "IDLEShortRun": "0",
        "IDLELid": "0",
        "IDLENoGCD": "0",
        "BadRun": "0",
        "FailedRun": "0"
    },
    "jobs_prev_states": {
        // Same attributes like in jobs_states
    },
    "failures": [
        {
            "sub_run": "1",
            "failures": "11",
            "job_id": "3913834"
        }
    ],
    "validated": true,
    "submitted": true,
    "last_status_change": "2014-11-07 06:16:36",
    "error_message": {
        "ERROR": [ ],
        "FAILED": [
            {
                "job_id": "3913834",
                "sub_run": "1",
                "submitdir": "/tmp/i3filter/1877/iceprod_3484.1415360812.122805",
                "log_tails": [
                    {
                        "file": "",
                        "content": "too many errors.2014-11-07 06:14:11'Configuring log4cplus.....ok06:13:33 - IceTray externs.....ok06:13:33 - IceTray 0 iteration 0.....failed"
                    },
                    {
                        "file": "log4cplus",
                        "content": ""
                    },
                    {
                        "file": "icetray.003484.log",
                        "content": ""
                    },
                    {
                        "file": "stdout",
                        "content": ""
                    },
                    {
                        "file": "iceprod.dataset_1877.queue_3484.out",
                        "content": ", (&quot;I3DAQDecodeException&quot;, 1), (&quot;LID&quot;, 1), (&quot;SDST_FilterMinBias_13&quot;, 5), (&quot;SDST_InIceSMT_IceTopCoincidence_13&quot;, 1), (&quot;SDST_IceTopSTA3_13&quot;, 1), (&quot;SDST_IceTop_InFill_STA3_13&quot;, 1)]\nWill keep all events."
                    },
                    {
                        "file": "stderr",
                        "content": ""
                    },
                    {
                        "file": "iceprod.dataset_1877.queue_3484.err",
                        "content": "(I3SimpleFitter): (OnlineL2_SplineMPE) finishing after 0th physics frame (I3SimpleFitter.cxx:462 in virtual void I3SimpleFitter::Finish())\nWARN (I3SimpleFitter): (OnlineL2_SplineMPE) 0 seeds, 0 good fits, 0 events with at least one good fit (I3SimpleFitter.cxx:464 in virtual void I3SimpleFitter::Finish())\nRemoving cpandel parameterization ... done\nError: IceTray exited with status (256)'"
                    }
                ]
            }
        ]
    },
    "snapshot_id": "90",
    "production_version": "0"

}</pre>
              <p>
                Since the most elements are self explanatory, in the following are just a few things explained in more detail.
              </p>
                <ul>
                  <li>
                    <code>status</code>
                    <p>
                      It has two values: <code>name</code> and <code>value</code>. Both are representing the same state but the <code>value</code> is of the type <code>int</code> and can compared easily.
                    </p>
                    <p>
                      The following states are supported for each run (<code>name</code> (<code>value</code>)):
                    </p>
                    <p>
                      <code>NONE</code> (0), <code>OK</code> (1), <code>IDLE</code> (2), <code>PROCESSING</code> (3), <code>PROCESSING/ERRORS</code> (4), and <code>FAILED</code> (5).
                    </p>
                    <div class="alert alert-warning" role="alert">
                      <strong>Warning:</strong> The state <code>NONE</code> means that the run is only in preparation. It is not submitted, processed, processing or something else. Consequently, <code>job_states</code>, <code>jobs_prev_states</code>, <code>failures</code>, <code>last_status_change</code>, and <code>error_message</code> are empty.
                    </div>
                  </li>
                  <li>
                    <code>jobs_states</code> &amp; <code>jobs_prev_states</code>
                    <p>
                      It contains all available job state and the number of jobs/sub runs that are in each state.
                    </p>
                    <div class="alert alert-info" role="alert">
                      <strong>Note:</strong> These attributes will be empty if the run status is <code>NONE</code>.
                    </div>
                  </li>
                  <li>
                    <code>submitted</code>
                    <p>
                      Indicates if the run has been submitted to iceprod.
                    </p>
                    <div class="alert alert-warning" role="alert">
                      <strong>Warning:</strong> It does not mean that the run is not processed. The run can have the state <code>OK</code> without be submitted (e.g. if the run was processed on cobalt and not via iceprod).
                    </div>
                  </li>
                  <li>
                    <code>validated</code>
                    <p>
                      Indicates if the run has been successfully validated.
                    </p>
                    <div class="alert alert-info" role="alert">
                      <strong>Note:</strong> Only use runs that are validated for analysis. If it is a L2 dataset, it means that the run is in the good run list.
                    </div>
                    <div class="alert alert-warning" role="alert">
                      <strong>Warning:</strong> L3 datasets earlier than season 2015 does not have a validated flag. Therefore, they are set as validated as default.
                    </div>
                  </li>
                </ul>
              </li>
              <li>
                <code>datasets</code>
                <p>The data structure of the response is shown below:</p>
<pre>datasets": {
    ...
    "1872": {
        "dataset_id": "1872",
        "description": "IC86_2012_TestRuns_In_IC86_2011",
        "selected": false,
        "supported": false,
        "season": null
        "type": null
    },
    ...
    "1888": {
        "dataset_id": "1888",
        "description": "IC86_2016 Offline Production",
        "selected": false,
        "supported": true,
        "season": "2016",
        "type": "L2"
    
    },
    "1889": {
        "dataset_id": "1889",
        "description": "IC86 2016 L3 Data Production for the Muon WG",
        "selected": false,
        "supported": true,
        "season": "2016",
        "type": "L3"
    }
    ...
}</pre>
                <p>In the following a few parameters are explained in more detail:</p>
                <ul>
                  <li>
                    <code>supported</code>
                    <p>
                      Some datasets aren't supported by the server. This can have several reasons:
                    </p>
                    <ul>
                      <li>Old data: Information live in other tables/data sources dat isn't implemented yet.</li>
                      <li>Not L2/3 dataset: So far only L2 and L3 datasets are supported.</li>
                    </ul>
                  </li>
                  <li>
                    <code>season</code>
                    <p>
                      If this dataset is supported it will always be linked to a season. A season is always a year and you'll find the season in the <code>seasons</code>
                      list. If the dataset isn't supported, it usually has the value <code>null</code>.
                    </p>
                  </li>
                  <li>
                    <code>type</code>
                    <p>
                      Currently, only L2 and L3 datasets are supported. If this dataset isn't supported, it will have the value <code>null</code>.
                    </p>
                  </li>
                  <li>
                    <code>selected</code>
                    <p>
                      This field is <code>true</code> if you selected this dataset via the parameter <code>dataset_id</code>.
                    </p>
                  </li>
                </ul>
              </li>
              <li>
                <code>seasons</code>
                <p>The data structure of the response is shown below:</p>
<pre>seasons": {
    ...
    "2015": {
        "season": "2015",
        "first_run": "126378",
        "test_runs": [
            "126289",
            "126290",
            "126291"
        ]
    },
    "2016": {
        "season": "2016",
        "first_run": "127950",
        "test_runs": [
            "127891",
            "127892",
            "127893"
        ]
    }
    ...
}</pre>
                <p>All attributes should be self explanatory.</p>
              </li>
            </ul>
          </div>
        </div>
      </div>
    </div>

    <div class="modal fade" id="jm-dialog-day" tabindex="-1" role="dialog" aria-labelledby="jm-dialog-label">
      <div class="modal-dialog modal-lg" role="document">
        <div class="modal-content">
          <div class="modal-header">
            <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
            <h4 class="modal-title" id="jm-dialog-day-label">Dialog title</h4>
          </div>
          <div class="modal-body">
          </div>
        </div>
      </div>
    </div>

    <div class="modal fade" id="jm-dialog" tabindex="-1" role="dialog" aria-labelledby="jm-dialog-label">
      <div class="modal-dialog modal-lg" role="document">
        <div class="modal-content">
          <div class="modal-header">
            <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
            <h4 class="modal-title" id="jm-dialog-label">Dialog title</h4>
          </div>
          <div class="modal-body">
            <div class="row">
                <div class="col-md-4" id="jm-dialog-log-menu"></div>
                <div class="col-md-8" id="jm-dialog-log-content"></div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </body>
</html>
