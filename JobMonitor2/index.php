<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Offline Processing JobMonitor 2</title>

    <!-- Bootstrap -->
    <link href="css/bootstrap.min.css" rel="stylesheet">
    <link href="css/jobmonitor2.css" rel="stylesheet">

    <!-- HTML5 shim and Respond.js for IE8 support of HTML5 elements and media queries -->
    <!-- WARNING: Respond.js doesn't work if you view the page via file:// -->
    <!--[if lt IE 9]>
      <script src="https://oss.maxcdn.com/html5shiv/3.7.2/html5shiv.min.js"></script>
      <script src="https://oss.maxcdn.com/respond/1.4.2/respond.min.js"></script>
    <![endif]-->

    <script src="js/jquery.min.js"></script>
    <script src="js/bootstrap.min.js"></script>
    <script src="js/class.jobmonitor.calendar.js"></script>
    <script src="js/class.jobmonitor.updater.js"></script>
    <script src="js/class.jobmonitor.js"></script>
    <script src="js/jobmonitor.js"></script>
  </head>
  <body>
    <nav class="navbar navbar-default">
      <div class="container-fluid">
        <!-- Brand and toggle get grouped for better mobile display -->
        <div class="navbar-header">
          <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#bs-example-navbar-collapse-1" aria-expanded="false">
            <span class="sr-only">Toggle navigation</span>
            <span class="icon-bar"></span>
            <span class="icon-bar"></span>
            <span class="icon-bar"></span>
          </button>
          <a class="navbar-brand" href="#">Offline Processing Job Monitor 2</a>
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
            <li><a href="#" id="force-update"><span class="glyphicon glyphicon-refresh" aria-hidden="true"></span></a></li>
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
            <li class="dropdown">
              <a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-haspopup="true" aria-expanded="false"><b>Dataset:</b> 1883 <span class="caret"></span></a>
              <ul class="dropdown-menu">
                <li><a href="#">Action</a></li>
                <li><a href="#">Another action</a></li>
              </ul>
            </li>
          </ul>
        </div><!-- /.navbar-collapse -->
      </div><!-- /.container-fluid -->
    </nav>

    <div id="content-frame">
      <div class="panel panel-default">
        <div class="panel-heading">
          <h3 class="panel-title">Calendar View</h3>
        </div>
        <div class="panel-body">
          Panel content
        </div>
      </div>    
    </div>
  </body>
</html>
