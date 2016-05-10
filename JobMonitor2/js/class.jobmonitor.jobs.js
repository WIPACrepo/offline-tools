
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
}

JobMonitorJobs.prototype = new JobMonitorView('current-jobs');
JobMonitorJobs.prototype.constructor = JobMonitorJobs;

JobMonitorJobs.prototype.updateView = function(data) {
    var iam = this;
    var html = this._createTableHeader();

    html += '<tbody>';

    $.each(data['runs'], function(runId, value) {
        html += iam._createRunEntry(runId, value);
    });

    html += '</tbody>';

    html += this._createTableFooter();

    $(this.getContent()).html(html);
    $('table', this.getContent()).DataTable(this.tableOptions);
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

    var html = '<tr>'
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

