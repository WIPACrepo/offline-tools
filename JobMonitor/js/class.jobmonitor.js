
/**
 * Creates an instance of JobMonitor.
 *
 * @this {JobMonitor}
 */
function JobMonitor(params) {
    params = typeof params !== 'undefined' ? params : {};

    /** @private */ this.timer = undefined;
    /** @private */ this.datatable = undefined;
    /** @private */ this.datatable_completed = undefined;
    /** @private */ this.last_update_time = undefined;
    /** @private */ this.update_enabled = true;
    /** @private */ this.update_list = {'calendar': true, 'current_jobs': false, 'completed_jobs': false};
    /** @private */ this.container = $('#current_jobs');
    /** @private */ this.completed_jobs = $('#completed_jobs');

    /** @private */ this.calendar = $('#calendar');
    /** @private */ this.weekdays = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    /** @private */ this.months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];

    /** @private */ this.legends = {'Processed': 'day-ok',
                                    'Processed w/ Errors': 'day-error',
                                    'Processing': 'day-proc',
                                    'Processing w/ Errors': 'day-proc-error',
                                    'Not All Runs Submitted Yet': 'day-not-all-submitted',
                                    'L2 Not Validated Yet': 'day-not-validated'};

    /** @private */ this.extended_data = $('#extended_data');
    /** @private */ this.extended_data_tabs = $('#tabs', this.extended_data)

    /** @private */ this.cj_length_selection = 'compl_jobs' in params ? params['compl_jobs'] : [10, 20, 30, 50, 100];
    /** @private */ this.cj_length_default = 'compl_jobs_default' in params ? params['compl_jobs_default'] : 10;
    /** @private */ this.cj_length = $('select[name="completed_job_length"]');

    /** @private */ this.update_interval_selection = 'up_inter' in params ? params['up_inter'] : [1, 5, 10, 20, 30, 60, 90, 120, -1];
    /** @private */ this.update_interval_default = 'up_inter_default' in params ? params['up_inter_default'] : -1;
    /** @private */ this.update_interval = $('#update_interval select');
    /** @private */ this.last_update_text = $('#last_update strong');
    /** @private */ this.last_update_loading = $('#last_update img');
    /** @private */ this.last_updated = $('#last_update span');

    /** @private */ this.datasets = $('#dataset_id select');
    /** @private */ this.update_now = $('#update_now');
    /** @private */ this.dataset_default = 'dataset_default' in params ? params['dataset_default'] : 1883;

    /** @private */ this.data_url = 'query.php';

    /** @private */ this.errors = $('#error span');

    /** @private */ this.tooltips = {};

    /** @private */ this.cookie_pref = {'expires': 365 * 10};

    /** @private */ this.md5sums = {};
    /** @private */ this.changes = false;
    /** @private */ this.change_indicator = $('#change_indicator');

    /** @private */ this.page_title = document.title;
}

/**
 * Tries to get the cookie value. If the value is undefined, default_value is returned.
 *
 * @private
 * @param {string} name The cookie key
 * @param {string} default_value The default value
 * @returns {string} The cookie or default value
 */
JobMonitor.prototype._get_cookie = function(name, default_value) {
    var value = Cookies.get(name);

    if(typeof value === 'undefined') {
        return default_value;
    } else {
        if(value === 'true') {
            return true;
        } else if(value === 'false') {
            return false;
        }

        return value;
    }
}

/**
 * Initializes and starts the JobMonitor.
 */
JobMonitor.prototype.run = function() {
    // Set default values
    // Sections
    for(var section in this.update_list) {
        this.update_list[section] = this._get_cookie(section, this.update_list[section]);
    }

    // Update interval
    this.update_interval_default = this._get_cookie('update_interval', this.update_interval_default);

    // Dataset
    this.dataset_default = this._get_cookie('dataset', this.dataset_default);

    // Create HTML for tables
    var html = '<table class="display" cellspacing="0" width="100%">';
    html += this._table_x('thead');
    html += '<tbody></tbody>';
    html += this._table_x('tfoot');
    html += '</table>';

    // Initialize
    $(this.container).html(html);
    this.datatable = $('table', this.container).DataTable({
        'order': [[9, 'desc']],
        'lengthMenu': [[10, 25, 50, -1], [10, 25, 50, 'All']]
    });
    
    $(this.completed_jobs).html(html);
    this.datatable_completed =  $('table', this.completed_jobs).DataTable({
        'order': [[9, 'desc']],
        'lengthMenu': [[10, 25, 50, -1], [10, 25, 50, 'All']]
    });

    $(this.extended_data).dialog({'resizable': true, 
                                  'autoOpen': false,
                                  'modal': true,
                                  'width': this.get_doc_width(.8),
                                  'height': this.get_doc_height(.8)});
    $(this.extended_data_tabs).tabs();

    // Workaround
    var iam = this;

    // How many runs should be loaded for 'Recently completed jobs'?
    this.cj_length_selection.forEach(function(n) {
        $(iam.cj_length).append($('<option></option>').attr('value', n).text('Show ' + n.toString() + ' Jobs'));
    });

    // How often should the view be updated
    this.update_interval_selection.forEach(function(n) {
        var text = '';

        if(n < 1) {
            text = 'never';
        } else if(n < 60) {
            text = n.toString() + ' min';
        } else if(n >= 60) {
            var hours = Math.floor(n / 60);
            var minutes = n - hours * 60;

            text = hours.toString() + ' h'
            
            if(minutes > 0) {
                text += ' ' + minutes + ' min';
            }
        }

        $(iam.update_interval).append($('<option></option>').attr('value', n).text(text));
    });

    // Enable toggle
    $('.toggle').each(function () {
        $(this).append('<div class="toggle-indicator open"></div>');

        var indicator = $('.toggle-indicator', $(this));
        var content = $('.toggle-content', $(this));

        var action = function() {
            $(content).slideToggle('slow', function() {
                var open = false;

                if($(content).css('display') === 'none') {
                    $(indicator).removeClass('open').addClass('close');
                } else {
                    open = true;
                    $(indicator).removeClass('close').addClass('open');
                }

                iam.update_list[$(content).attr('id')] = open;

                // Store status in cookie
                Cookies.set($(content).attr('id'), open, iam.cookie_pref);
                console.log('(Cookie) Set section ' + $(content).attr('id') + ' = ' + open);

                // If section is opened, reload
                if(open) {
                    iam.update_data(true);
                }
            });
        };

        // Set default
        if(iam.update_list[$(content).attr('id')]) {
            $(content).show();
        } else {
            $(content).hide();
        }

        // Add action
        $('.captain', $(this)).click(action);
        $(indicator).click(action);
    });

    // Invoke updates
    $(this.cj_length).change(function() {
       iam.update_data(true); 
    });


    $(this.update_interval).change(function() {
        iam.update_data(false);
        Cookies.set('update_interval', $(this).val(), iam.cookie_pref);
    });

    // If a new dataset is selected, reload data
    $(this.datasets).change(function() {
        iam.update_data(true);
        Cookies.set('dataset', $(this).val(), iam.cookie_pref);
    });

    // Give the update_now button the power...
    $(this.update_now).click(function() {
        iam.update_data(true);
    });

    // Set defaults
    $(this.update_interval).val(this.update_interval_default);
    $(this.cj_length).val(this.cj_length_default);
    $(this.datasets).val(this.dataset_default);

    // Load data for the first time
    this.update_data(true);

    // Enable tool tips
    $(document).tooltip({
        'content': function() {
            var title = $(this).attr('title');
            if(title in iam.tooltips) {
                return iam.tooltips[title];
            } else {
                return title;
            }
        }
    });

    // Hook for click in document to catch an user action
    
    var change_seen = function() {
        iam.changes = false;
        iam._handle_change_indicator();
    };

    $(document).click(change_seen);
    $(document).scroll(change_seen);

    // Timer for updateing the elapsed time
    setInterval(function() {iam._update_last_update();}, 1000 * 60); // Update it every minute
}

/**
 * Returns the current with of the document.
 *
 * @param {number} p The width is multiplicated with this number. Default is 1.0.
 * @return {integer} Current width of the document.
 */
JobMonitor.prototype.get_doc_width = function(p) {
    p = typeof p !== 'undefined' ? p : 1.0;
    return Math.round($(window).width() * p);
}

/**
 * Returns the current height of the document.
 *
 * @param {number} p The height is multiplicated with this number. Default is 1.0.
 * @return {integer} Current height of the document.
 */
JobMonitor.prototype.get_doc_height = function(p) {
    p = typeof p !== 'undefined' ? p : 1.0;
    return Math.round($(window).height() * p);
}

/**
 * Creates a table header or footer.
 *
 * @private
 * @param {string} x Pass only thead or tfoot.
 * @return {string} The HTML.
 */
JobMonitor.prototype._table_x = function(x) {
    return '<' + x + '><tr>'
        + '<td>Run #</td>'
        + '<td>OK</td>'
        + '<td>FAILED</td>'
        + '<td>PROCESSING</td>'
        + '<td># of jobs</td>'
        + '<td>Status</td>'
        + '<td>Previous Status</td>'
        + '<td>Failures</td>'
        + '<td>Run Date</td>'
        + '<td>Last Status Change</td>'
        + '</tr></' + x + '>';
}

/**
 * Updates the data.
 *
 * @param {bool} force If set to true, it will update the data despite the interval is set to 'never'. Default is false.
 */
JobMonitor.prototype.update_data = function(force) {
    force = typeof force !== 'undefined' ? force : false;

    var iam = this;

    if(this.update_enabled) {
        var time = $(this.update_interval).val();
    
        if(this.timer !== undefined) {
            clearInterval(this.timer);
            console.log('Cleared timer')
        }
   
        if(time >= 1 || force) {
            console.log('Update forced');
            this._run_update();
        }
 
        if(time >= 1) {
            console.log('Set Interval to ' + time.toString() + ' min')
            this.timer = setInterval(function () {iam._run_update();}, time * 60 * 1000);
        }
    }
}

/**
 * Runs the actual update.
 *
 * @private
 */
JobMonitor.prototype._run_update = function() {
    var iam = this;

    var dataset_id = $(this.datasets).val();
    var completed_job_length = $(this.cj_length).val();

    console.log('Run update: ' + (new Date()));

    var options = '';
    for(var key in this.update_list) {
        var value = this.update_list[key];

        if(value) {
            if(options.length > 0) {
                options += ',';
            }

            options += key;
        }
    }

    console.log('Options: ' + options);

    this._loading(true);
    $.getJSON(this.data_url, {'dataset_id': dataset_id,
                              'completed_job_length': completed_job_length,
                              'options': options}, 
        function(data) {
            iam._update_view(data);
    })
    .fail(function() {
            $(iam.errors).html('Receiving information failed.');
    })
    .always(function() {
        iam._loading(false);
    });
    
}

/**
 * Sets the views to the loading state.
 *
 * @private
 * @param {bool} start if start = true, then start of loading. If false, then end of loading.
 */
JobMonitor.prototype._loading = function(start) {
    if(start) {
        this.update_enabled = false;
        $(this.last_update_text).hide();
        $(this.last_update_loading).show();
        $(this.datasets).attr('disabled', true);
        $(this.update_interval).attr('disabled', true);
        $(this.cj_length).attr('disabled', 'true');
    } else {
        this.update_enabled = true;
        $(this.last_update_text).show();
        $(this.last_update_loading).hide();
        $(this.datasets).removeAttr('disabled');
        $(this.update_interval).removeAttr('disabled');
        $(this.cj_length).removeAttr('disabled');
    }
}

/**
 * Adds additional info for failed jobs.
 *
 * @private
 * @param {row} row The row of Datatable.
 * @param {object} data The data
 */
JobMonitor.prototype._add_extended_info = function(row, data) {
    var iam = this;

    $(row).data('extended_info', data['extended_info']);

    if(data['num_status_failed'] + data['num_status_error'] > 0) {
        $('td', row).eq(5).attr('title', 'FAILED: ' + data['num_status_failed'] + ', ERROR: ' + data['num_status_error']);
        $(row).click(function() {
            iam._fill_extended_data(this);
            $(iam.extended_data).dialog('option', 'height', iam.get_doc_height(.8));
            $(iam.extended_data).dialog('option', 'width', iam.get_doc_width(.8));
            $(iam.extended_data).dialog('open');
        });
    }
}

/**
 * Fills the dialog with the actual data.
 *
 * @private
 * @param {row} row The row of Datatable
 */
JobMonitor.prototype._fill_extended_data = function(row) {
    var iam = this;

    var data = $(row).data('extended_info');

    $('select', this.extended_data).html('<optgroup label="Failed"></optgroup><optgroup label="Error"></optgroup>');
    var global_job_nr = 0;
    for(var i = 0; i < data['failed'].length; ++i) {
        $('select optgroup[label="Failed"]', this.extended_data).append($('<option></option>').attr('value', global_job_nr++).data('d', data['failed'][i]).text(data['failed'][i]['job_id']))
    }

    for(var i = 0; i < data['error'].length; ++i) {
        $('select optgroup[label="Error"]', this.extended_data).append($('<option></option>').attr('value', global_job_nr++).data('d', data['error'][i]).text(data['error'][i]['job_id']))
    }

    $('select', this.extended_data).change(function() {
        var tabs = $('#tabs', iam.extended_data);
        $(tabs).tabs('destroy');
        $(tabs).html('');
        $(tabs).append('<ul></ul>');
        var ul = $('ul', tabs);
        
        var jobdata = $(this.options[this.selectedIndex]).data('d');

        for(var i = 0; i < jobdata['log_tails'].length; ++i) {
            var filename = jobdata['log_tails'][i]['file'].split('/').pop();
            if('' === filename) {
                filename = '(LOG)';
            }

            $(ul).append($('<li></li>').html('<a href="#file-' + i + '">' + filename + '</a>'));
        }

       for(var i = 0; i < jobdata['log_tails'].length; ++i) {
            var filename = jobdata['log_tails'][i]['file'].split('/').pop();
            var html = '<div id="file-' + i + '">';

            if(filename !== '' && filename !== 'stdout' && filename !== 'stderr' && filename !== 'log4cplus') {
                html += '<div class="filepath"><input type="text" value="' + jobdata['submitdir'] + '/' + jobdata['log_tails'][i]['file'] + '" readonly /></div>';
            }

            if(jobdata['log_tails'][i]['content'] === '') {
                html += '<div class="log-empty">Empty Log</div>'
            } else {
                html += '<div class="log">' + jobdata['log_tails'][i]['content'] + '</div>';
            }

            $(tabs).append(html);
            $('.filepath input', tabs).click(function() {
                $(this).select();
            });
        }

        $(tabs).tabs(); 
    });

    $('select', this.extended_data).val(0).change();
}

/**
 * Updates the table.
 *
 * @private
 * @param {Datatable} table The Datatable
 * @param {object} data The data
 */
JobMonitor.prototype._update_table = function(table, data) {
    var runs_in_table = [];

    table.rows().iterator('row', function (context, index) {
        var run_id = $('td', this.row(index).node()).eq(0).text();

        // RunId, row index, inducator if entry is still in server response
        runs_in_table.push([run_id, index, false]);
    });

    for(var i = 0; i < data.length; ++i) {
        var r = data[i];

        var row = runs_in_table.filter(function(e, i) {
            if(e[0] == r['run_id']) {
                runs_in_table[i][2] = true;
                return true;
            } else {
                return false;
            }
        });

        if(row.length > 0) {
            row = row[0][1];
        } else {
            row = undefined;
        }

        var rowdata = [ r['run_id'],
                        r['num_status_ok'], 
                        r['num_status_failed'], 
                        r['num_status_processing'], 
                        r['num_of_jobs'], 
                        r['status'], 
                        r['prev_state'], 
                        r['failures'],
                        r['date'], 
                        r['last_status_change']
                      ];

        if(row !== undefined) {
            table.row(row).data(rowdata).draw();
            row = table.row(row).node();
        } else {
            row = table.row.add(rowdata).draw(false).node();
        }

        if(r['num_status_failed'] + r['num_status_error'] > 0) {
            $(row).addClass('failed');
        } else if($(row).hasClass('failed')) {
            $(row).removeClass('failed');
        }

        this._add_extended_info(row, r);
    }

    table.rows(runs_in_table.filter(function(e, i) {
            return !e[2];
        }).map(function(e, i) {
            return e[1];
        })
    ).remove().draw();
}

/**
 * Updates the view.
 *
 * @private
 * @param {object} data The data
 */
JobMonitor.prototype._update_view = function(data) {
    if(data['error']) {
        $(this.errors).html(data['error_msg']);
        return;
    }

    if('current' in data['data'] && this._data_changed('current', data)) {
        console.log('New data found for current jobs');
        this.changes = true;
        this._update_table(this.datatable, data['data']['current']);
    }
    
    if('completed' in data['data'] && this._data_changed('completed', data)) {
        console.log('New data found for completed jobs');
        this.changes = true;
        this._update_table(this.datatable_completed, data['data']['completed']);
    }

    if('calendar' in data['data'] && this._data_changed('calendar', data)) {
        console.log('New data found for the calendar');
        this.changes = true;
        this._update_calendar(data['data']['calendar']);
    }

    for(var type in data['md5']) {
        this.md5sums[type] = data['md5'][type];
    }

    console.log('Run _update_view: ' + (new Date()));

    this._handle_change_indicator();

    this.last_update_time = new Date();
    $(this.last_updated).html(this.last_update_time.toLocaleString('en-US'));
    this._update_last_update();
}

/**
 * Sets the change indicator visible or hides it depending on the variable this.changes.
 */
JobMonitor.prototype._handle_change_indicator = function() {
    if(this.changes) {
        $(this.change_indicator).show();

        document.title = '! ' + this.page_title;
    } else {
        $(this.change_indicator).hide();

        document.title = this.page_title;
    }
}

/**
 * Compares two md5 sums if they exist. If they are equl, false is returned. Otherwise, true.
 * If no md5 sum is stored at the client side or no md5 sum is provided by the server, always true is returned.
 *
 * @private
 * @param {string} type 'calendar', 'current', or 'completed'
 * @param {object} data The received data
 *
 * @returns {bool} True if new data is available.
 */
JobMonitor.prototype._data_changed = function(type, data) {
    if(type in this.md5sums && type in data['md5']) {
        return this.md5sums[type] !== data['md5'][type];
    } else {
        return true;
    }
}

/**
 * Creates the calendar view.
 *
 * @private
 */
JobMonitor.prototype._update_calendar = function(data) {
    var iam = this;

    var create_legend = function() {
        var html = '<div id="legend">';

        for(var label in iam.legends) {
            html += '<span class="label"><span class="icon ' + iam.legends[label] + '"></span>' + label + '</span>';
        }

        html += '</div>';

        return html;
    };

    var get_day_status = function(data) {
        if(typeof data === 'undefined') {
            return '';
        }

        var ok = data['OK'] + data['IDLEShortRun'] + data['IDLENoFiles']
                + data['IDLETestRun'] + data['IDLELid'] + data['BadRun']
                + data['FailedRun'];

        var failed = ok + data['FAILED'];

        //console.log(ok + ' == ' + data['jobs']);

        if(ok == data['jobs']) {
            return 'day-ok';
        } else if(failed == data['jobs']) {
            return 'day-error';
        } else if(data['FAILED'] + data['ERROR'] > 0) {
            return 'day-proc-error';
        } else if(data['jobs'] > 0) {
            return 'day-proc';
        } else {
            return '';
        }
    };

    var create_tooltip = function(year, month, day, data, raw_data) {
        if(typeof data === 'undefined') {
            return '';
        }

        var html = '<table>';
        html += '<thead>';
        html += '<tr>';
        html += '<td colspan="4">';
        html += 'Runs from ' + iam.months[month - 1] + ' ' + day + ', ' + year + ':';
        html += '</td>';
        html += '</tr>';
        html += '<tr>';
        html += '<td>';
        html += 'Run';
        html += '</td>';
        html += '<td>';
        html += 'Subm.';
        html += '</td>';
        html += '<td>';
        html += 'Val.';
        html += '</td>';
        html += '<td>';
        html += 'Proc.';
        html += '</td>';
        html += '</tr>';
        html += '</thead>';
        html += '<tbody>';

        data['grl'].forEach(function(run) {
            html += '<tr>';
            html += '<td>';
            html += run;
            html += '</td>';

            if($.inArray(run, data['submitted_runs']) !== -1) {
                html += '<td class="run-ok">';
                html += '&#10003;';
            } else {
                html += '<td class="run-bad">';
                html += '&#10007;';
            }

            html += '</td>';
            
            if($.inArray(run, raw_data['not_validated']) !== -1) {
                html += '<td class="run-bad">';
                html += '&#10007;';
            } else {
                html += '<td class="run-ok">';
                html += '&#10003;';
            }

            html += '</td>';

            if($.inArray(run, raw_data['proc_error']) !== -1) {
                html += '<td class="run-bad">';
                html += 'ERROR';
            } else if($.inArray(run, raw_data['proc']) !== -1) {
                html += '<td>';
                html += 'PROC/IDLE';
            } else {
                html += '<td class="run-ok">';
                html += 'OK';
            }

            html += '</td>';
            html += '</tr>';
        });

        html += '</tbody>';
        html += '</table>';
        return html;
    };

    /**
     * Creates the HTML for a month.
     *
     * @param {string} year The year
     * @param {string} month The Month
     * @param {string} day The day
     * @param {object} data The data of this month
     * @param {object} raw_data All calendar data
     */
    var create_month = function(year, month, data, raw_data) {
        // Month starts at 0!
        var first_week_day = (new Date(year, month - 1, 1)).getDay();

        // Get last day of month, therefore no -1!
        var days_of_month = (new Date(year, month, 0)).getDate();

        var html = '<table class="highlight">';
        html += '<thead>';
        html += '<tr>';
        html += '<td colspan="7">';
        html +=  iam.months[month - 1]+ ' ' + year;
        html += '</td>';
        html += '</tr>';
        html += '<tr>';

        for(var i = 0; i < iam.weekdays.length; ++i) {
            html += '<td>';
            html += iam.weekdays[i];
            html += '</td>';
        }

        html += '</tr>';
        html += '</thead>';
        html += '<tbody>';

        var day = -1;
        while(day <= days_of_month) {
            html += '<tr>';

            for(var dow = 0; dow < 7; ++dow) {
                if(day < 0 && dow == first_week_day) {
                    day = 1;
                }

                if(day < 0 || day > days_of_month) {
                    html += '<td></td>';
                } else {
                    var tooltip = create_tooltip(year, month, day, data[day], raw_data);

                    var classes = ' class="';
                    classes += get_day_status(data[day]);

                    if(typeof data[day] !== 'undefined') {
                        // Don't compare it for equal numbers since failed runs can be submitted that aren't in the GRL!
                        if(data[day]['submitted_runs'].length < data[day]['grl'].length) {
                            classes += ' day-not-all-submitted';
                        }

                        var validated = true;
                        data[day]['grl'].some(function(run) {
                            if($.inArray(run, raw_data['not_validated']) !== -1) {
                                validated = false;
                                // stop loop
                                return true;
                            } else {
                                return false;
                            }
                        });

                        if(!validated) {
                            classes += ' day-not-validated';
                        }
                    }

                    classes += '"';

                    if(tooltip !== '') {
                        var tt_key = 'tt_key-' + year + '-' + month + '-' + day + '';
                        iam.tooltips[tt_key] = tooltip;
                        html += '<td' + classes + ' title="' + tt_key + '">' + day + '</td>';
                    } else {
                        html += '<td>' + day + '</td>';
                    }

                    ++day;
                }
            }

            html += '</tr>';
        }

        html += '</tbody>';
        html += '</table>';

        return html;
    };

    var html = '';

    $.each(data, function(year, months) {
        if(year === 'not_validated' || year === 'proc_error' || year === 'proc') {
            // Skip no year vars
            return;
        }

        $.each(months, function(month, days) {
            html += create_month(year, month, days, data);
        });
    });

    html += create_legend();

    this.calendar.html(html);
}

/**
 * Converts seconds into '#d ##h ##m ##s'.
 *
 * @param {int} seconds Time in seconds
 * @param {bool} leading_zero If true, it adds a leading zero to each number (except for days). Default is true.
 * @param {bool} show_seconds If true, it adds the seconds to the string. Otherwise, only days, hours and minutes. Default is true.
 *
 * @returns {string} The converted time.
 */
JobMonitor.prototype.convert_seconds_to_string = function(seconds, leading_zero, show_seconds) {
    show_seconds = typeof show_seconds === 'undefined' ? true : show_seconds;
    leading_zero = typeof leading_zero === 'undefined' ? true : leading_zero;

    var days = 0;
    var hours = 0;
    var minutes = 0;

    if(seconds >  59) {
        minutes = Math.floor(seconds / 60.);
        seconds -= minutes * 60;
    }

    if(minutes > 59) {
        hours = Math.floor(minutes / 60.);
        minutes -= hours * 60;
    }

    if(hours > 23) {
        days = Math.floor(hours / 24.);
        hours -= hours * 24;
    }

    if(leading_zero) {
        var add_zero = function(num) {
            if(num < 10) {
                return '0' + num.toString();
            } else {
                return num;
            }
        };

        hours = add_zero(hours);
        minutes = add_zero(minutes);
        seconds = add_zero(seconds);
    }

    // Build string
    var str = '';
    if(days > 0) {
        str += days.toString() + 'd ';
    }

    if(hours > 0) {
        str += hours.toString() + 'h ';
    }

    if(minutes > 0) {
        str += minutes.toString() + 'm ';
    }

    if(show_seconds) {
        str += seconds.toString() + 's';
    }

    return str.trim();
}

/**
 * Updates the 'Last Update' time in the view.
 *
 * @private
 */
JobMonitor.prototype._update_last_update = function() {
    if(this.last_update_time == undefined) {
        return;
    }

    var diff_text = '';
    var diff = (new Date()) - this.last_update_time;

    // convert into minutes
    diff = Math.round(diff / 1000. / 60);

    if(diff < 1) {
        diff_text = 'just this minute';
    } else {
        diff_text = this.convert_seconds_to_string(diff * 60, false, false) + ' ago';
    }

    $(this.last_update_text).html('(' + diff_text + ')');
}

