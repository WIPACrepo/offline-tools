
function JobMonitorCalendar(url) {
    this.url = url;

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
                                'In Preparation': 'day-none',
                                'Not All Runs Submitted Yet': 'day-not-all-submitted',
                                'Not All Runs Validated Yet': 'day-not-validated'};

    this.calendarData = undefined;
    
    var iam = this;
    this.selectedDay = undefined;

    $('#jm-dialog-day').on('show.bs.modal', function (event) {
        iam._dayDialog($('.modal-title', this), $('.modal-body', this));
    }).on('hidden.bs.popover', function () {
        iam.url.removeState('day');
        iam.url.pushState();
    });
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

    // Handle detailed day info
    $('td[data-jm-day]', this.getContent()).click(function() {
        var dateSplit = $(this).attr('data-jm-day').split('-');

        if(dateSplit.length === 3) {
            try {
                iam.selectedDay = {'year': parseInt(dateSplit[0]),
                                   'month': parseInt(dateSplit[1]),
                                   'day': parseInt(dateSplit[2])
                                  };

                $('#jm-dialog-day').modal();

                iam.url.setState('day', $(this).attr('data-jm-day'));
                iam.url.pushState();
            } catch(e) {
                console.log(e);
                // Do nothing when parsing failed
            }
        }

        return false;
    });

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

    // Close popover on click outside of popover
    $('body').on('click', function (e) {
        $('[data-toggle="popover"]', iam.getContent()).each(function () {
            if (!$(this).is(e.target) && $(this).has(e.target).length === 0 && $('.popover').has(e.target).length === 0) {
                $(this).popover('hide');
            }
        });
    });

    var preselectedDay = this.url.getState('day');

    if(typeof preselectedDay !== 'undefined') {
        var dayFound = false;

        $('td[data-jm-day=\'' + preselectedDay + '\']', this.getContent()).each(function() {
            dayFound = true;
            $(this).click();
        });

        if(!dayFound) {
            this.url.removeState('day');
            this.url.pushState();
        }
    }
}

JobMonitorCalendar.prototype._dayDialog = function(header, content) {
    header.html('Runs from ' + this.months[this.selectedDay['month'] - 1] + ' ' + this.selectedDay['day'] + ', ' + this.selectedDay['year']);

    content.html(this._createDaySummaryTable(this.selectedDay['year'], this.selectedDay['month'], this.selectedDay['day'], true));
    
    $('[data-toggle="popover"]', content).popover({'html': true});
    
    // Close popover on click outside of popover
    $('body').on('click', function (e) {
        $('[data-toggle="popover"]', content).each(function () {
            if (!$(this).is(e.target) && $(this).has(e.target).length === 0 && $('.popover').has(e.target).length === 0) {
                $(this).popover('hide');
            }
        });
    });
}

JobMonitorCalendar.prototype._createDaySummaryTable = function(year, month, day, verbose) {
    verbose = typeof verbose === 'undefined' ? false : verbose;
    var dayData = this.calendarData[year][month][day];
    var iam = this;

    if(typeof dayData === 'undefined') {
        return '';
    }

    var html = '<table class="jm-day-summary' + (verbose ? ' table table-striped' : '') + '">';
    html += '<thead>';
    html += '<tr>';
    html += '<th>';
    html += 'Run';
    html += '</th>';

    if(verbose) {
        html += '<th>';
        html += 'Snapshot Id';
        html += '</th>';

        html += '<th>';
        html += 'Production Version';
        html += '</th>';
    }

    html += '<th>';
    html += verbose ? 'Submitted' : 'Subm.';
    html += '</th>';
    html += '<th>';
    html += verbose ? 'Validated' : 'Val.';
    html += '</th>';

    if(verbose) {
        html += '<th>';
        html += 'Processing Completed';
        html += '</th>';
        html += '<th>';
        html += '';
        html += '</th>';
    }

    html += '</tr>';
    html += '</thead>';
    html += '<tbody>';

    dayData['runs'].forEach(function(run) {
        var runStatusCSS = iam.runStatusCSSMapping[run['status']['name']];
        var progressIndicator = Math.floor(run['jobs_states']['OK'] / run['sub_runs'] * 100);

        var folderPath = '/data/exp/IceCube/' + year + '/filtered/level2/' + (month < 10 ? '0' : '') + month + (day < 10 ? '0' : '') + day + '/Run00' + run['run_id'] + '/';

        html += '<tr>';
        html += '<td>';
        html += run['run_id'];

        if(!isNaN(progressIndicator)) {
            html += '<span class="run-status-indicator ' + runStatusCSS + '">' + progressIndicator + '%</span>';
        }

        html += '</td>';

        if(verbose) {
            html += '<td>';
            html += run['snapshot_id'];
            html += '</td>';
            html += '<td>';
            html += run['production_version'];
            html += '</td>';
        }

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
        
        if(verbose) {
            html += '<td>';
            html += run['status']['name'] === 'OK' ? run['last_status_change'] : 'N/A';
            html += '</td>';
            html += '<td>';

            if(run['validated']) {
                html += '<a href="#" onclick="return false" data-container="body" data-toggle="popover" data-placement="bottom" data-content="<b>Folder:</b> <code>' + folderPath + '</code>">';
                html += '<span class="glyphicon glyphicon-folder-open" aria-hidden="true"></a>';
            }

            html += '</td>';
        }

        html += '</tr>';
    });

    html += '</tbody>';
    html += '</table>';

    if(!verbose) {
        html += '<small class="text-muted">Click on day for more information</small>';
    }

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

    var html = '<table>';
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
