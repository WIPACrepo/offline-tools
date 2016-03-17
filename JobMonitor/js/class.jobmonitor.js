
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
    /** @private */ this.container = $('#current_jobs');
    /** @private */ this.completed_jobs = $('#completed_jobs');

    /** @private */ this.extended_data = $('#extended_data');
    /** @private */ this.extended_data_tabs = $('#tabs', this.extended_data)

    /** @private */ this.cj_length_selection = 'compl_jobs' in params ? params['compl_jobs'] : [10, 20, 30, 50, 100];
    /** @private */ this.cj_length_default = 'compl_jobs_default' in params ? params['compl_jobs_default'] : 10;
    /** @private */ this.cj_length = $('select[name="completed_job_length"]');

    /** @private */ this.update_interval_selection = 'up_inter' in params ? params['up_inter'] : [1, 5, 10, 20, 30, 60, 90, 120, -1];
    /** @private */ this.update_interval_default = 'up_inter_default' in params ? params['up_inter_default'] : 10;
    /** @private */ this.update_interval = $('#update_interval select');
    /** @private */ this.last_update_text = $('#last_update strong');
    /** @private */ this.last_update_loading = $('#last_update img');
    /** @private */ this.last_updated = $('#last_update span');

    /** @private */ this.datasets = $('#dataset_id select');
    /** @private */ this.update_now = $('#update_now');

    /** @private */ this.data_url = 'query.php';

    /** @private */ this.errors = $('#error span');

    // Initialize
    this._init();
}

/**
 * Initializes the JobMonitor.
 *
 * @private
 */
JobMonitor.prototype._init = function() {
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

    // Invoke updates
    $(this.cj_length).change(function() {
       iam.update_data(true); 
    });


    $(this.update_interval).change(function() {
       iam.update_data(false); 
    });

    // If a new dataset is selected, reload data
    $(this.datasets).change(function() {
        iam.update_data(true);
    });

    // Give the update_now button the power...
    $(this.update_now).click(function() {
        iam.update_data(true);
    });

    // Set defaults
    $(this.update_interval).val(this.update_interval_default);
    $(this.cj_length).val(this.cj_length_default);

    // Load data for the first time
    this.update_data(true);

    // Enable tool tips
    $(document).tooltip();

    // Timer for updateing the elapsed time
    setInterval(this._update_last_update, 1000 * 60); // Update it every minute
}

/**
 * Returns the current with of the document.
 *
 * @param {number} p The width is multiplicated with this number. Default is 1.0.
 * @return {integer} Current width of the document.
 */
JobMonitor.prototype.get_doc_width = function(p) {
    p = typeof p !== 'undefined' ? p : 1.0;
    return Math.round($(document).width() * p);
}

/**
 * Returns the current height of the document.
 *
 * @param {number} p The height is multiplicated with this number. Default is 1.0.
 * @return {integer} Current height of the document.
 */
JobMonitor.prototype.get_doc_height = function(p) {
    p = typeof p !== 'undefined' ? p : 1.0;
    return Math.round($(document).height() * p);
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
            console.log('Set Interval to ' + time.toString())
            this.timer = setInterval(this.run_update, time * 60 * 1000);
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

    this._loading(true);
    $.getJSON(this.data_url, {'dataset_id': dataset_id,
                              'completed_job_length': completed_job_length}, 
        function(data) {
        iam._update_view(data)
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

    this._update_table(this.datatable, data['data']['current']);
    this._update_table(this.datatable_completed, data['data']['completed']);

    this.last_update_time = new Date();
    $(this.last_updated).html(this.last_update_time.toLocaleString());
    this._update_last_update();
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
    diff = diff / 1000. / 60;
    
    if(diff < 1) {
        diff_text = 'just this minute';
    } else {
        diff_text = Math.round(diff).toString() + ' min ago';
    }

    $(this.last_update_text).html('(' + diff_text + ')');
}

