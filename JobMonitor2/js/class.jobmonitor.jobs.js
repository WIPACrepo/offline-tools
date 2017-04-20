
function JobMonitorJobs(url) {
    var iam = this;

    this.url = url;

    this.dataCache = undefined;

    this.footerSumColumns = [1, 2, 3, 4];

    this.jobOptions = {'completed': false,
                        'failed': true,
                        'processing': true,
                        'idling': true};

    // Initialize jobOptions with URL values
    $.each(iam.jobOptions, function(name, value) {
        iam.jobOptions[name] = iam.url.getState('show' + name[0].toUpperCase() + name.substring(1) + 'Jobs', String(iam.jobOptions[name])).toLowerCase() === 'true';
    });
    

    this.tableOptions = {
        'lengthMenu': [[10, 25, 50, -1], [10, 25, 50, 'All']],
        'footerCallback': function(row, data, start, end, display) {
            var api = this.api();
            iam._createFooter(api, row, data, start, end, display);
        },
        'order': [[9, 'desc']],
        'dom': 'lf<"toolbar">rtip',
        'initComplete': function(){
            iam._createToolbar();
        }
    };

    this.data = undefined;
    this.dialogSelectedRun = undefined;

    this.jobStatusCSSMapping = {
        'NONE': '',
        'IDLE': 'idle',
        'OK': 'ok',
        'PROCESSING': 'proc',
        'PROCESSING/ERRORS': 'error',
        'FAILED': 'failed'
    };

    $('#jm-dialog').on('show.bs.modal', function (event) {
        iam._logDialog($('.modal-title', this), $('.modal-body', this), iam.data['runs'][iam.dialogSelectedRun]);
    });
}

JobMonitorJobs.prototype = new JobMonitorView('current-jobs');
JobMonitorJobs.prototype.constructor = JobMonitorJobs;

JobMonitorJobs.prototype.updateView = function(data) {
    this.dataCache = data;

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

JobMonitorJobs.prototype._createToolbar = function() {
    var iam = this;

    var html = '';

    html += '<div class="checkbox"><input type="checkbox" data-dest="completed" id="jm-jobs-show-completed-jobs"><label for="jm-jobs-show-completed-jobs">Show completed jobs</label></div>';
    html += '<div class="checkbox"><input type="checkbox" data-dest="failed" id="jm-jobs-show-failed-jobs" checked><label for="jm-jobs-show-failed-jobs">Show failed jobs</label></div>';
    html += '<div class="checkbox"><input type="checkbox" data-dest="processing" id="jm-jobs-show-processing-jobs" checked><label for="jm-jobs-show-processing-jobs">Show processing jobs</label></div>';
    html += '<div class="checkbox"><input type="checkbox" data-dest="idling" id="jm-jobs-show-idling-jobs" checked><label for="jm-jobs-show-idling-jobs">Show idling jobs</label></div>';

    var toolbar = $('div.toolbar', this.getContent()).html(html);

    $(':checkbox', toolbar).each(function() {
        var key = $(this).attr('data-dest');
        $(this).prop('checked', iam.jobOptions[key]);
    });

    $(':checkbox', toolbar).change(function() {
        var key = $(this).attr('data-dest');

        if(key in iam.jobOptions) {
            iam.jobOptions[key] = $(this).prop('checked');
        } else {
            console.log('Unknown key "' + key + '"');
        }

        // Propagate status to url
        $.each(iam.jobOptions, function(name, value) {
            iam.url.setState('show' + name[0].toUpperCase() + name.substring(1) + 'Jobs', value);
        });

        iam.url.pushState();

        if(typeof iam.dataCache !== 'undefined') {
            iam.updateView(iam.dataCache);
            $('select').filter(function() {return !$(this).hasClass('selectpicker');}).addClass('selectpicker').selectpicker();
        }
    });

    // Number of shown entries drop down
    $('select[name="example_length"]').change(function() {
        iam.url.setState('jobsLength', $(this).val());
        iam.url.pushState();
    });

    var jobsLength = this.url.getState('jobsLength', this.tableOptions['lengthMenu'][0][0]);
    $('select[name="example_length"]').val(jobsLength).change();
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

    var sortedKeys = Object.keys(states).sort(function(a,b) {return states[b] - states[a];});

    $.each(sortedKeys, function(index, state) {
        var num = states[state];

        if(num > 0) {
            var color = 'default';

            if(state == 'FAILED') {
                color = 'danger';
            } else if(state == 'ERROR') {
                color = 'warning';
            } else if(state == 'OK') {
                color = 'success';
            } else if(state == 'PROCESSING') {
                color = 'primary';
            } else if(state == 'COPIED' || state == 'COPYING') {
                color = 'info';
            }

            list.push('<span class="label label-' + color + '">' + state + ' (' + num + ')</span>');
        }
    });

    return list.join(' ');
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
    // Display only runs that have a specific status. See jobOptions.
    // Check for values in resources/class.ProcessingJobs.php
    switch(value['status']['value']) {
        case 0:
            // Preparation state: Nothing to display
            return '';
            break;

        case 1:
            // 'OK' status
            if(!this.jobOptions['completed']) {
                return '';
            }
            break;

        case 2:
            // 'IDLE' status
            if(!this.jobOptions['idling']) {
                return '';
            }
            break;

    
        case 3:
        case 4:
            // 'PROCESSING' and 'PROCESSING/ERRORS' status
            if(!this.jobOptions['processing']) {
                return '';
            }
            break;

        case 5:
            // 'FAILED" status
            if(!this.jobOptions['failed']) {
                return '';
            }
            break;
    }

    var lastStatusChange = value['last_status_change'];

    if(null == lastStatusChange) {
        lastStatusChange = '';
    }

    var classes = [];
    var jobState = '';
    if(value['jobs_states']['FAILED'] > 0) {
        classes.push('failed');
        classes.push('loginfo');
    } else if(value['jobs_states']['ERROR'] > 0) {
        classes.push('error');
        classes.push('loginfo');
    } else if(value['status']['name'] === 'IDLE') {
        classes.push('idle');
    } else if(value['status']['name'] === 'OK') {
        classes.push('ok');
    } else if(value['status']['name'] === 'PROCESSING') {
        classes.push('proc');
    }

    var jobState = this.jobStatusCSSMapping[value['status']['name']];

    var progressIndicator = Math.floor(value['jobs_states']['OK'] / value['sub_runs'] * 100);

    var badRun = !value['good_i3'] && !value['good_it'];

    if(badRun) {
        classes.push('bad-run');
    }

    var html = '<tr class="' + classes.join(' ') + '">'
        + '<td class="hidden-xs hidden-sm">' + runId + (value['24h_test_run'] ? '<sup>T</sup>' : '') + '</td>'
        + '<td class="hidden-xs hidden-sm">' + value['jobs_states']['OK'] + '</td>'
        + '<td class="hidden-xs hidden-sm">' + value['jobs_states']['FAILED'] + '</td>'
        + '<td class="hidden-xs hidden-sm">' + value['jobs_states']['PROCESSING'] + '</td>'
        + '<td class="hidden-xs hidden-sm">' + value['sub_runs'] + '</td>'
        + '<td class="hidden-xs hidden-sm">' + this._createStatusList(value['jobs_states']) + '</td>'
        + '<td class="hidden-xs hidden-sm">' + this._createStatusList(value['jobs_prev_states']) + '</td>'
        + '<td class="hidden-xs hidden-sm">' + this._createFailureList(value['failures']) + '</td>'
        + '<td class="hidden-xs hidden-sm">' + value['date'] + '</td>'
        + '<td class="hidden-xs hidden-sm">' + lastStatusChange + '</td>'
        + '<td class="hidden-md hidden-lg ' + jobState + (badRun ? ' bad-run' : '') + '">' + runId + (value['24h_test_run'] ? '<sup>T</sup>' : '') + (isNaN(progressIndicator) ? '' : ' (' + progressIndicator + '%)') + '</td>'
        + '</tr>';

    return html;
}

JobMonitorJobs.prototype._createTableHeader = function() {
    var html = '<table id="example" class="table table-striped table-bordered" cellspacing="0" width="100%">';

    html += '<thead><tr>'
        + '<th class="hidden-xs hidden-sm">Run #</th>'
        + '<th class="hidden-xs hidden-sm">OK</th>'
        + '<th class="hidden-xs hidden-sm">FAILED</th>'
        + '<th class="hidden-xs hidden-sm">PROCESSING</th>'
        + '<th class="hidden-xs hidden-sm"># of jobs</th>'
        + '<th class="hidden-xs hidden-sm">Status</th>'
        + '<th class="hidden-xs hidden-sm">Previous Status</th>'
        + '<th class="hidden-xs hidden-sm">Failures</th>'
        + '<th class="hidden-xs hidden-sm">Run Date</th>'
        + '<th class="hidden-xs hidden-sm">Last Status Change</th>'
        + '<th class="hidden-md hidden-lg">Run #</th>'
        + '</tr>'
        + '</thead>';

    return html;
}

JobMonitorJobs.prototype._createTableFooter = function() {
    var html = '<tfoot class="hidden-xs hidden-sm"><tr>'
        + '<th>Sums</th>'
        + '<th></th>'
        + '<th></th>'
        + '<th></th>'
        + '<th></th>'
        + '<th colspan="5"></th>'
        + '</tr>'
        + '</tfoot>';

    html += '</table>';

    html += '<small class="text-muted"><sup>T</sup>: 24h Test Run</small><br/>';
    html += '<small class="text-muted"><span style="color: red;">123456</span>: Run marked as bad</small>';

    return html;
}

JobMonitorJobs.prototype._logDialog = function(header, content, data) {
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
