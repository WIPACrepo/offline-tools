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
    <link href="css/dataTables.bootstrap.min.css" rel="stylesheet">
    <link href="css/font-awesome.min.css" rel="stylesheet">
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
    <script src="js/dataTables.bootstrap.min.js"></script>
    <script src="js/class.jobmonitor.view.js"></script>
    <script src="js/class.jobmonitor.calendar.js"></script>
    <script src="js/class.jobmonitor.jobs.js"></script>
    <script src="js/class.jobmonitor.updater.js"></script>
    <script src="js/class.jobmonitor.datasets.js"></script>
    <script src="js/class.jobmonitor.js"></script>
    <script src="js/jobmonitor.js"></script>
  </head>
  <body>
    <div class="jm-invisible" id="jm-api-version"><?php print($CONFIG['api_version']); ?></div>
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
            <li class="dropdown" id="jm-view-dropdown">
              <a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-haspopup="true" aria-expanded="false">View <span class="caret"></span></a>
              <ul class="dropdown-menu">
                <li><a href="#"><span class="glyphicon glyphicon-unchecked" aria-hidden="true"></span> Calendar</a></li>
                <li><a href="#"><span class="glyphicon glyphicon-unchecked" aria-hidden="true"></span> Processing/Failed Jobs</a></li>
                <li><a href="#"><span class="glyphicon glyphicon-unchecked" aria-hidden="true"></span> Recently Successfully Processed Jobs</a></li>
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
                <li><a href="#">Feedback</a></li>
                <li><a href="#">API</a></li>
                <li role="separator" class="divider"></li>
                <li><a href="#"><?php print($CONFIG['svn']['rev']); ?></a></li>
              </ul>
            </li>
          </ul>
        </div><!-- /.navbar-collapse -->
      </div><!-- /.container-fluid -->
    </nav>

    <div id="content-frame">
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
          <h3 class="panel-title jm-view-header">Currently Processing or Failed Jobs</h3>
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
  </body>
</html>
