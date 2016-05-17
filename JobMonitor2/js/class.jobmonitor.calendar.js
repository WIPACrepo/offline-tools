
function JobMonitorCalendar() {
    /** @private */ this.weekdays = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    /** @private */ this.months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
    /**
     * Mapping of status and CSS class for days
     *
     * @private
     */
    this.dayStatusCSSMapping = {
        'NONE': 'day-none',
        'IDLE': 'day-idle',
        'OK': 'day-ok',
        'PROCESSING': 'day-proc',
        'PROCESSING/ERRORS': 'day-proc-error',
        'FAILED': 'day-error'
    };
    /**
     * Mapping of status and CSS class for runs
     *
     * @private
     */
    this.runStatusCSSMapping = {
        'NONE': '',
        'IDLE': 'run-idle',
        'OK': 'run-ok',
        'PROCESSING': 'run-proc',
        'PROCESSING/ERRORS': 'run-proc-error',
        'FAILED': 'run-error'
    };
    /** @private */ this.key = {'Processed': 'day-ok',
                                'Processed w/ Errors': 'day-error',
                                'Processing': 'day-proc',
                                'Processing w/ Errors': 'day-proc-error',
                                'Jobs Idling': 'day-idle',
                                'Not All Runs Submitted Yet': 'day-not-all-submitted',
                                'Not All Runs Validated Yet': 'day-not-validated'};

    this.calendarData = undefined;
}

JobMonitorCalendar.prototype = new JobMonitorView('calendar');
JobMonitorCalendar.prototype.constructor = JobMonitorCalendar;

JobMonitorCalendar.prototype.updateView = function(data) {
    var iam = this;
    this.calendarData = this._computeCalendarData(data['runs']);
    var html = '';

    $.each(this.calendarData, function(year, yearData) {
        $.each(yearData, function(month, monthData) {
            html += iam._createMonth(year, month, monthData);
        });
    });

    html += this._createKey();

    $(this.getContent()).html(html);

    // Handle tooltips
    $('[data-toggle=\'popover\']', this.getContent()).popover(
        {
            'trigger': 'hover',
            'container': 'body',
            'placement': 'bottom',
            'html': true,
            'title': function() {
                var dateSplit = $(this).attr('data-jm-day').split('-');

                if(dateSplit.length === 3) {
                    try {
                        var year = parseInt(dateSplit[0]);
                        var month = parseInt(dateSplit[1]);
                        var day = parseInt(dateSplit[2]);
                        return 'Runs from ' + iam.months[month - 1] + ' ' + day + ', ' + year + ':';
                    } catch(e) {
                        console.log('catch ' + e);
                        // Do nothing when parsing failed
                    }
                }
            },
            'content': function() {
                var dateSplit = $(this).attr('data-jm-day').split('-');

                if(dateSplit.length === 3) {
                    try {
                        return iam._createDaySummaryTable(parseInt(dateSplit[0]), parseInt(dateSplit[1]), parseInt(dateSplit[2]));
                    } catch(e) {
                        // Do nothing when parsing failed
                    }
                }
            }
        }
    );
}

JobMonitorCalendar.prototype._createDaySummaryTable = function(year, month, day) {
    var dayData = this.calendarData[year][month][day];
    var iam = this;

    if(typeof dayData === 'undefined') {
        return '';
    }

    var html = '<table class="jm-day-summary">';
    html += '<thead>';
    html += '<tr>';
    html += '<th>';
    html += 'Run';
    html += '</th>';
    html += '<th>';
    html += 'Subm.';
    html += '</th>';
    html += '<th>';
    html += 'Val.';
    html += '</th>';
    html += '</tr>';
    html += '</thead>';
    html += '<tbody>';

    dayData['runs'].forEach(function(run) {
        var runStatusCSS = iam.runStatusCSSMapping[run['status']['name']];
        var progressIndicator = Math.floor(run['jobs_states']['OK'] / run['sub_runs'] * 100);

        html += '<tr>';
        html += '<td>';
        html += run['run_id'];

        if(!isNaN(progressIndicator)) {
            html += '<span class="run-status-indicator ' + runStatusCSS + '">' + progressIndicator + '%</span>';
        }

        html += '</td>';

        if(run['submitted']) {
            html += '<td class="run-ok">';
            html += '&#10003;';
        } else {
            html += '<td class="run-bad">';
            html += '&#10007;';
        }

        html += '</td>';
        
        if(run['validated']) {
            html += '<td class="run-ok">';
            html += '&#10003;';
        } else {
            html += '<td class="run-bad">';
            html += '&#10007;';
        }

        html += '</td>';
        html += '</tr>';
    });

    html += '</tbody>';
    html += '</table>';
    return html;
}

JobMonitorCalendar.prototype._createKey = function() {
    var html = '<div id="key">';
 
    for(var label in this.key) {
        html += '<span class="jm-label"><span class="icon ' + this.key[label] + '"></span>' + label + '</span>';
    }
 
    html += '</div>';
 
    return html;
}

JobMonitorCalendar.prototype._computeCalendarData = function(data) {
    // Store data in a tree: year -> month -> day = list of runs
    var calendar = {};

    $.each(data, function(runId, value) {
        var dateSplit = value['date'].split('-');
        var d = {
            'year': parseInt(dateSplit[0]),
            'month': parseInt(dateSplit[1]), 
            'day': parseInt(dateSplit[2])
        };

        // Create day in data array
        if(!(d['year'] in calendar)) {
            calendar[d['year']] = {};
        }

        if(!(d['month'] in calendar[d['year']])) {
            calendar[d['year']][d['month']] = {};
        }

        if(!(d['day'] in calendar[d['year']][d['month']])) {
            // Status definition: see resources/class.ProcessingJobs.php
            calendar[d['year']][d['month']][d['day']] = {
                'summary': {
                    'datestr': value['date'],
                    'all_validated': true,
                    'all_submitted': true,
                    'status': 0,
                    'status_name': 'NONE'
                },
                'runs': []};
        }

        calendar[d['year']][d['month']][d['day']]['runs'].push(value);

        if(value['status']['value'] > calendar[d['year']][d['month']][d['day']]['summary']['status']) {
            calendar[d['year']][d['month']][d['day']]['summary']['status'] = value['status']['value'];
            calendar[d['year']][d['month']][d['day']]['summary']['status_name'] = value['status']['name'];
        }

        if(!value['submitted']) {
            calendar[d['year']][d['month']][d['day']]['summary']['all_submitted'] = false;
        } else if(!value['validated']) {
            calendar[d['year']][d['month']][d['day']]['summary']['all_validated'] = false;
        }
    });

    return calendar;
}

JobMonitorCalendar.prototype._createMonth = function(year, month, data) {
    // Month starts at 0!
    var firstWeekDay = (new Date(year, month - 1, 1)).getDay();

    // Get last day of month, therefore no -1!
    var daysOfMonth = (new Date(year, month, 0)).getDate();

    var html = '<table class="highlight">';
    html += '<thead>';
    html += '<tr>';
    html += '<td colspan="7">';
    html +=  this.months[month - 1]+ ' ' + year;
    html += '</td>';
    html += '</tr>';
    html += '<tr>';

    for(var i = 0; i < this.weekdays.length; ++i) {
        html += '<td>';
        html += this.weekdays[i];
        html += '</td>';
    }

    html += '</tr>';
    html += '</thead>';
    html += '<tbody>';

    var day = -1;
    while(day <= daysOfMonth) {
        html += '<tr>';

        for(var dow = 0; dow < 7; ++dow) {
            if(day < 0 && dow == firstWeekDay) {
                day = 1;
            }

            var dayData = data[day];

            if(day < 0 || day > daysOfMonth) {
                html += '<td class="no-day"></td>';
            } else {
                var classes = [];

                var tooltipAttr = '';

                if(typeof dayData !== 'undefined') {
                    // Handle run indicators
                    if(!dayData['summary']['all_validated']) {
                        classes.push('day-not-validated');
                    }

                    if(!dayData['summary']['all_submitted']) {
                        classes.push('day-not-all-submitted');
                    }

                    // Handle day states
                    if(this.dayStatusCSSMapping[dayData['summary']['status_name']].length > 0) {
                        classes.push(this.dayStatusCSSMapping[dayData['summary']['status_name']]);
                    }

                    tooltipAttr = ' data-toggle="popover" data-jm-day="' + dayData['summary']['datestr'] + '"';
                }

                html += '<td class="' + classes.join(' ') + '"' + tooltipAttr + '>' + day + '</td>';
                ++day;
            }
        }

        html += '</tr>';
    }

    html += '</tbody>';
    html += '</table>';

    return html;
}
