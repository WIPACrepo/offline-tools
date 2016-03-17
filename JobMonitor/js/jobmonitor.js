
$.fn.exists = function () {
    return this.length !== 0;
}

var datatable = undefined;
var datatable_completed = undefined;
var last_update_time = undefined;
var update_enabled = true;

$(document).ready(function() {
    $(document).tooltip();
    var dwidth = $(document).width();
    var dheight = $(document).height();
    $('#extended_data').dialog({'resizable': true, 'autoOpen': false, 'modal': true, 'width': dwidth * .8, 'height': dheight * .8});
    $('#extended_data #tabs').tabs();

    // Create HTML for tables
    var html = '<table class="display" cellspacing="0" width="100%">';
    html += table_header();
    html += '<tbody></tbody></table>';

    // Current jobs
    var container = $('#current_jobs');
    $(container).html(html);
    datatable = $('table', container).DataTable({
        'order': [[9, 'desc']],
        'lengthMenu': [[10, 25, 50, -1], [10, 25, 50, 'All']]
    });

    // Recently completed jobs
    var completed_jobs = $('#completed_jobs');
    $(completed_jobs).html(html);
    datatable_completed =  $('table', completed_jobs).DataTable({
        'order': [[9, 'desc']],
        'lengthMenu': [[10, 25, 50, -1], [10, 25, 50, 'All']]
    });

    // How many runs should be loaded for 'Recently completed jobs'?
    var cj_length = $('select[name="completed_job_length"]');
    for(var i = 1; i <= 20; ++i) {
        $(cj_length).append($('<option></option>').attr('value', 10 * i).text('Show ' + (10 * i).toString() + ' Jobs'));
    }

    // Handle timers & updates
    var update_interval = $('#update_interval select');
    for(var i = 1; i <= 30; ++i) {
        $(update_interval).append($('<option></option>').attr('value', i).text('' + i.toString() + ' min'));
    }

    $(cj_length).change(function() {
       $(update_interval).change(); 
    });

    var timer = undefined;

    $(update_interval).change(function() {
        if(update_enabled) {
            var time = $(this).val();

            if(timer !== undefined) {
                clearInterval(timer);
            }

            run_update();
            timer = setInterval(run_update, time * 60 * 1000);
        }
    });

    // If a new dataset is selected, reload. Fire a change on the update select
    // since it reloades the data.
    $('#dataset_id select').change(function() {
        $(update_interval).change();
    });

    // Give the update_now button the power...
    $('#update_now').click(function() {
        $(update_interval).change();
    });

    $(update_interval).val(10).change();

    // Timer for updateing the elapsed time
    setInterval(update_last_update, 1000 * 60); // Update it every minute
});

function run_update() {
    var dataset_id = $('#dataset_id select').val();
    var completed_job_length = $('select[name="completed_job_length"]').val();

    loading(true);
    $.getJSON('query.php', 
        {'dataset_id': dataset_id, 'completed_job_length': completed_job_length}, 
        update_view).fail(function() {
            $('#error span').html('Receiving information failed.');
    }).always(function() {loading(false);});
}

function loading(start) {
    // if start = true, then start of loading. If false, then end of loading.
    
    if(start) {
        update_enabled = false;
        $('#last_update strong').hide();
        $('#last_update img').show();
        $('#dataset_id select').attr('disabled', true);
        $('#update_interval select').attr('disabled', true);
        $('select[name="completed_job_length"]').attr('disabled', 'true');
    } else {
        update_enabled = true;
        $('#last_update strong').show();
        $('#last_update img').hide();
        $('#dataset_id select').removeAttr('disabled');
        $('#update_interval select').removeAttr('disabled');
        $('select[name="completed_job_length"]').removeAttr('disabled');
    }
}

function table_header() {
    return '<thead><tr>'
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
        + '</tr></thead>';
}

function add_extended_info(row, data) {
    $(row).data('extended_info', data['extended_info']);

    if(data['num_status_failed'] + data['num_status_error'] > 0) {
        $('td', row).eq(5).attr('title', 'FAILED: ' + data['num_status_failed'] + ', ERROR: ' + data['num_status_error']);
        $(row).click(function() {
            var dialog = $('#extended_data');
            fill_extended_data(dialog, this);
            $(dialog).dialog('open');
        });
    }
}

function fill_extended_data(dialog, row) {
    var data = $(row).data('extended_info');

    $('select', dialog).html('<optgroup label="Failed"></optgroup><optgroup label="Error"></optgroup>');
    var global_job_nr = 0;
    for(var i = 0; i < data['failed'].length; ++i) {
        $('select optgroup[label="Failed"]', dialog).append($('<option></option>').attr('value', global_job_nr++).data('d', data['failed'][i]).text(data['failed'][i]['job_id']))
    }

    for(var i = 0; i < data['error'].length; ++i) {
        $('select optgroup[label="Error"]', dialog).append($('<option></option>').attr('value', global_job_nr++).data('d', data['error'][i]).text(data['error'][i]['job_id']))
    }

    $('select', dialog).change(function() {
        var tabs = $('#tabs', dialog);
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

    $('select', dialog).val(0).change();
}

function update_table(table, data) {
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

        var rowdata = [r['run_id'], r['num_status_ok'], r['num_status_failed'], r['num_status_processing'], r['num_of_jobs'], r['status'], r['prev_state'], r['failures'], r['date'], r['last_status_change']];

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

        add_extended_info(row, r);
    }

    table.rows(runs_in_table.filter(function(e, i) {
            return !e[2];
        }).map(function(e, i) {
            return e[1];
        })
    ).remove().draw();
}

function update_view(data) {
    var error_container = $('#error');

    if(data['error']) {
        $(error_container).html(data['error_msg']);
        return;
    }

    var container = $('#current_jobs');
    var table = $('table', container);

    update_table(datatable, data['data']['current']);
    update_table(datatable_completed, data['data']['completed']);

    var last_updated = $('#last_update span');

    last_update_time = new Date();
    $(last_updated).html(last_update_time.toLocaleString());
    update_last_update();
}

function update_last_update() {
    if(last_update_time == undefined) {
        return;
    }

    var diff_text = '';
    var diff = (new Date()) - last_update_time;

    // convert into minutes
    diff = diff / 1000. / 60;
    
    if(diff < 1) {
        diff_text = 'just this minute';
    } else {
        diff_text = Math.round(diff).toString() + ' min ago';
    }

    $('#last_update strong').html('(' + diff_text + ')');
}

