
function JobMonitorJobs() {
    var iam = this;

    this.footerSumColumns = [1, 2, 3, 4];

    this.tableOptions = {
        'lengthMenu': [[10, 25, 50, -1], [10, 25, 50, 'All']],
        'footerCallback': function(row, data, start, end, display) {
            var api = this.api();
            iam._createFooter(api, row, data, start, end, display);
        },
        'order': [[9, 'desc']]
    };

    this.data = undefined;
    this.dialogSelectedRun = undefined;

    $('#jm-dialog').on('show.bs.modal', function (event) {
        iam._logDialog($('.modal-title', this), $('.modal-body', this), iam.data['runs'][iam.dialogSelectedRun]);
    });
}

JobMonitorJobs.prototype = new JobMonitorView('current-jobs');
JobMonitorJobs.prototype.constructor = JobMonitorJobs;

JobMonitorJobs.prototype.updateView = function(data) {
    var iam = this;
    var html = this._createTableHeader();

    this.data = data;

    html += '<tbody>';

    $.each(data['runs'], function(runId, value) {
        html += iam._createRunEntry(runId, value);
    });

    html += '</tbody>';

    html += this._createTableFooter();

    $(this.getContent()).html(html);
    $('table', this.getContent()).DataTable(this.tableOptions);

    $('tr.loginfo td', this.getContent()).click(function() {
        iam.dialogSelectedRun = $(this).parent().find('td:first-child').html();
        $('#jm-dialog').modal();
    });
}

JobMonitorJobs.prototype._createFooter = function(api, row, data, start, end, display) {
    this.footerSumColumns.forEach(function(column) {
        var total = api
            .column(column)
            .data()
            .reduce(function (a, b) {
                return parseInt(a) + parseInt(b);
            }, 0);

        $(api.column(column).footer()).html(total);
    });

    this.footerSumColumns.forEach(function(column) {
        if(column != 4) {
            try {
                var p = parseInt($(api.column(column).footer()).html()) / parseInt($(api.column(4).footer()).html()) * 100;

                if(!isNaN(p) && p > 0) {
                    $(api.column(column).footer()).html($(api.column(column).footer()).html() + ' (' + Math.floor(p) + '%)');
                }
            } catch(e) {
                // Do nothing
            }
        }
    });
}

JobMonitorJobs.prototype._createStatusList = function(states) {
    var list = [];

    $.each(states, function(state, num) {
        if(num > 0) {
            list.push(state);
        }
    });

    return list.join(',');
}

JobMonitorJobs.prototype._createFailureList = function(failures) {
    var list = [0];

    failures.forEach(function(failure) {
        if(-1 === $.inArray(failure['failures'], list)) {
            list.push(failure['failures']);
        }
    });

    return list.join(',');
}

JobMonitorJobs.prototype._createRunEntry = function(runId, value) {
    // Take only processing or failed/error jobs/runs
    // Check for values in resources/class.ProcessingJobs.php
    if(value['status']['value'] <= 1) {
        return '';
    }

    var lastStatusChange = value['last_status_change'];

    if(null == lastStatusChange) {
        lastStatusChange = '';
    }

    var classes = [];

    if(value['jobs_states']['FAILED'] > 0) {
        classes.push('failed');
        classes.push('loginfo');
    } else if(value['jobs_states']['ERROR'] > 0) {
        classes.push('error');
        classes.push('loginfo');
    }

    var html = '<tr class="' + classes.join(' ') + '">'
        + '<td>' + runId + '</td>'
        + '<td>' + value['jobs_states']['OK'] + '</td>'
        + '<td>' + value['jobs_states']['FAILED'] + '</td>'
        + '<td>' + value['jobs_states']['PROCESSING'] + '</td>'
        + '<td>' + value['sub_runs'] + '</td>'
        + '<td>' + this._createStatusList(value['jobs_states']) + '</td>'
        + '<td>' + this._createStatusList(value['jobs_prev_states']) + '</td>'
        + '<td>' + this._createFailureList(value['failures']) + '</td>'
        + '<td>' + value['date'] + '</td>'
        + '<td>' + lastStatusChange + '</td>'
        + '</tr>';

    return html;
}

JobMonitorJobs.prototype._createTableHeader = function() {
    var html = '<table id="example" class="table table-striped table-bordered" cellspacing="0" width="100%">';

    html += '<thead><tr>'
        + '<th>Run #</th>'
        + '<th>OK</th>'
        + '<th>FAILED</th>'
        + '<th>PROCESSING</th>'
        + '<th># of jobs</th>'
        + '<th>Status</th>'
        + '<th>Previous Status</th>'
        + '<th>Failures</th>'
        + '<th>Run Date</th>'
        + '<th>Last Status Change</th>'
        + '</tr>'
        + '</thead>';

    return html;
}

JobMonitorJobs.prototype._createTableFooter = function() {
    var html = '<tfoot><tr>'
        + '<th>Sums</th>'
        + '<th></th>'
        + '<th></th>'
        + '<th></th>'
        + '<th></th>'
        + '<th colspan="5"></th>'
        + '</tr>'
        + '</tfoot>';

    html += '</table>';

    return html;
}

JobMonitorJobs.prototype._logDialog = function(header, content, data) {
    console.log(data);

    var allLogs = data['error_message']['ERROR'].concat(data['error_message']['FAILED']);

    var menu = $('#jm-dialog-log-menu', content);
    var logs = $('#jm-dialog-log-content', content);

    header.html('Logs for run ' + data['run_id'] + ': ' + data['error_message']['ERROR'].length + ' errors, ' + data['error_message']['FAILED'].length + ' failures');

    var menuHtml = '<div id="jm-dialog-log-menu-files"></div><hr/>';

    var createMenu = function(subrun) {
        menuHtml += '<option value="' + subrun['job_id'] + '">Sub run ' + subrun['sub_run'] + ' (job #' + subrun['job_id'] + ')</option>';
    };

    var selectLogFile = function(select, data) {
        var selectedLogFile = $(select).val();

        var html = '';

        var fileName = data['log_tails'][selectedLogFile]['file'].trim();

        if(fileName !== '' && fileName !== 'log4cplus' && fileName !== 'stdout' && fileName !== 'stderr') {
            html += '<div class="alert alert-info" role="alert"><strong>File: </strong>';
            html += data['submitdir'] + '/' + data['log_tails'][selectedLogFile]['file'];
            html += '</div>';
        }

        if(data['log_tails'][selectedLogFile]['content'].trim() === '') {
            html += '<div class="alert alert-warning" role="alert">Empty log</div>';
        } else {
            html += '<div class="alert alert-warning" role="alert"><strong>Note:</strong> ';
            html += 'The shown log is only the tail that is stored in the database.';
            html += 'To see the entire log, find the file on submitter.';
            html += '</div>';
            html += '<pre class="log">';
            html += data['log_tails'][selectedLogFile]['content'];
            html += '</pre>';
        }

        logs.html(html);
    };

    var selectSubRun = function() {
        var selectedJobId = $(this).val();
        var selectedSubRun = undefined;

        allLogs.forEach(function(subrun) {
            if(subrun['job_id'] == selectedJobId) {
                selectedSubRun = subrun;
                return;
            }
        });

        console.log(selectedSubRun);

        var html = '<h5>Log</h5><select class="selectpicker">';

        var i = 0;
        selectedSubRun['log_tails'].forEach(function(log) {
            var fileName = log['file'];
            if(fileName === '') {
                fileName = '(Log)';
            }

            html += '<option value="' + i + '">' + fileName + '</option>';

            if(log['file'] === '' && selectedSubRun['log_tails'].length > 1) {
                html += '<option data-divider="true"></option>';
            }
            
            ++i;
        });

        html += '</select>';

        $('#jm-dialog-log-menu-files', menu).html(html)
                                            .find('.selectpicker')
                                            .selectpicker()
                                            .change(function() {selectLogFile(this, selectedSubRun);})
                                            .change();
    };

    if(data['error_message']['ERROR'].length) {
        menuHtml += '<h5>Errors</h5>';
        menuHtml += '<select class="selectpicker">';
        data['error_message']['ERROR'].forEach(createMenu);
        menuHtml += '</select>';
    }


    if(data['error_message']['FAILED'].length) {
        menuHtml += '<h5>Failures</h5>';
        menuHtml += '<select class="selectpicker">';
        data['error_message']['FAILED'].forEach(createMenu);
        menuHtml += '</select>';
    }

    menu.html(menuHtml);
    $('select', menu).change(selectSubRun);
    $('select', menu).change();
    $('.selectpicker', menu).selectpicker();
}

