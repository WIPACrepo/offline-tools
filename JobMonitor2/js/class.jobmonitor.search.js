
function JobMonitorSearch(url, findSeasonByRunIdCallback, findDatasetsBySeasonCallback, searchUrl, apiCompatibilityCheckerCallback) {
    this.url = url;
    this.findSeasonByRunIdCallback = findSeasonByRunIdCallback;
    this.findDatasetsBySeasonCallback = findDatasetsBySeasonCallback;
    this.searchUrl = searchUrl;
    this.apiCompatibilityCheckerCallback = apiCompatibilityCheckerCallback;

    this.inputRunId = $('#jm-dialog-search #jm-search-run-id');
    this.inputEventId = $('#jm-dialog-search #jm-search-event-id');
    this.inputEventSwitch = $('#jm-dialog-search #jm-search-event-id-switch');
    this.eventIdWrapper = $('#jm-dialog-search #jm-search-event-id-wrapper');
    this.submitButton = $('#jm-dialog-search #jm-search-buttonbar button');
    this.resultBox = $('#jm-dialog-search #jm-search-result-box');
    this.modal = $('#jm-dialog-search');
    this.searchingIndicator = $('#jm-dialog-search #jm-search-buttonbar #jm-searching-indicator');

    this.calendar = $('#jm-content-view-calendar .jm-view-content');

    this.data = undefined;

    this.searching = false;
}

JobMonitorSearch.prototype.init = function() {
    var iam = this;

    this.inputEventSwitch.change(function() {
        if($(this).prop('checked')) {
            iam.eventIdWrapper.show('slow');
        } else {
            iam.eventIdWrapper.hide('slow');
        }

        iam.url.setState('searchEventId', $(this).prop('checked'));
        iam.url.pushState();
    });

    var initState = this.url.getState('searchEventId', 'false').toLowerCase();

    if(initState !== 'true' && initState !== 'false') {
        this.url.removeState('searchEventId');
    } else {
        if(initState === 'true') {
            this.inputEventSwitch.prop('checked', true);
            this.inputEventSwitch.change();
        }
    }

    this.submitButton.click(function() {
        iam.search();
    });

    this.inputRunId.keyup(function(e) {
        if(e.keyCode === 13) {
            iam.submitButton.click();
        }
    });

    this.inputEventId.keyup(function(e) {
        if(e.keyCode === 13) {
            iam.submitButton.click();
        }
    });
}

JobMonitorSearch.prototype.search = function() {
    // Check search mode: only run or also run id?
    // If only the run is searched just check the data
    // and open the day of the calendar view.
    var eventIdMode = this.inputEventSwitch.prop('checked');

    var runId = this.inputRunId.val();
    var eventId = this.inputEventId.val();

    if(eventIdMode && eventId.trim() !== '') {
        this._searchRequest(runId, eventId);
    } else {
        this._searchRequest(runId);
    }
}

JobMonitorSearch.prototype._searchRequest = function(runId, eventId) {
    if(this.searching) {
        return;
    }

    this.searching = true;

    this._startSearching();

    var iam = this;
    var params = {'run_id': runId};


    if(typeof eventId !== 'undefined') {
        params['event_id'] = eventId;
    }

    $.getJSON(this.searchUrl, params, 
        function(data) {
            iam._searchCompleted(data);
    })
    .fail(function() {
            iam._error('An error occurred.');
    })
    .always(function() {
        iam._endSearching();

        iam.searching = false;
    });
}

JobMonitorSearch.prototype._searchCompleted = function(data) {
    var iam = this;
    data = typeof data === 'undefined' ? {} : data;

    if(Object.keys(data).length === 0 ||
        typeof data['error'] === 'undefined' ||
        typeof data['error_msg'] === 'undefined' ||
        typeof data['error_trace'] === 'undefined' ||
        typeof data['api_version'] === 'undefined' ||
        typeof data['data'] === 'undefined' ||
        typeof data['data']['query'] === 'undefined' ||
        typeof data['data']['result'] === 'undefined') {
        this._error('Received a non-interpretable answer');
        console.log(data);
        return;
    }

    if(!this.apiCompatibilityCheckerCallback(data['api_version'])) {
        console.log(data);
        this._error('Client version is not compatible with data. Force a page reload and try again.');
        return;
    }

    if(data['error']) {
        this._error(data['error_msg']);
        console.log(data);
        return;
    }

    if(!data['data']['result']['successfully']) {
        this._noResult(data['data']['result']['message']);
    } else {
        this._somethingFound(data['data']['result']['message']);
    }

    // Season and dataset information
    var season =  this.findSeasonByRunIdCallback(data['data']['query']['run_id']);
    var datasets = [];

    // If no season has been found, -1 is returned
    if(season > -1) {
        datasets = this.findDatasetsBySeasonCallback(season);
    }

    // Ok, we have a result. Let's build the result HTML...
    var eventIdSearch = typeof data['data']['query']['event_id'] !== 'undefined';

    var html = '';

    html += '<table class="table">';
    html += '<thead>';
    html += '<tr>';
    html += '<th colspan="2">';
    html += '<strong>Result</strong>';
    html += '</th>';
    html += '</tr>';
    html += '</thead>';
    html += '<tbody>';
    html += '<tr>';
    html += '<td>';
    html += '<strong>Run Id</strong>';
    html += '</td>';
    html += '<td>';
    html += data['data']['query']['run_id'];
    html += '</td>';
    html += '</tr>';
    html += '<tr>';
    html += '<td>';
    html += '<strong>Status</strong>';
    html += '</td>';
    html += '<td>';
    html += 'InIce: ' + (data['data']['result']['good_i3'] ? '<span class="run-ok">&#10003;</span>' : '<span class="run-bad">&#10007;</span>') + ' ';
    html += 'IceTop: ' + (data['data']['result']['good_it'] ? '<span class="run-ok">&#10003;</span>' : '<span class="run-bad">&#10007;</span>') + ' ';
    html += '<a href="https://live.icecube.wisc.edu/run/' + data['data']['query']['run_id'] + '/" target="_blank" data-container="body" data-toggle="tooltip" data-placement="bottom" title="More run information on i3live">';
    html += '<span class="glyphicon glyphicon-info-sign" aria-hidden="true"></a>';
    html += '</td>';
    html += '<tr>';
    html += '<td>';
    html += '<strong>Date</strong>';
    html += '</td>';
    html += '<td>';
    html += data['data']['result']['date'];
    html += '</td>';
    html += '</tr>';
    html += '</tr>';

    if(eventIdSearch) {
        html += '<tr>';
        html += '<td>';
        html += '<strong>Event Id</strong>';
        html += '</td>';
        html += '<td>';
        html += data['data']['query']['event_id'];
        html += '</td>';
        html += '</tr>';
    }

    if(data['data']['result']['successfully']) {
        html += '<tr>';
        html += '<td>';
        html += '<strong>Season</strong>';
        html += '</td>';
        html += '<td>';
        html += (season > -1 ? season : 'Unknown');
        html += '</td>';
        html += '</tr>';
    }

    html += '<tr>';
    html += '<td>';
    html += '<strong>Datasets</strong>';
    html += '</td>';
    html += '<td>';
    
    if(datasets.length > 0 && data['data']['result']['successfully']) {
        var url = this.url.getUrlWithoutQueryString() + '?';
        var states = this.url.getStates();

        datasets.forEach(function(dataset, index) {
            // Set current dataset id in states object
            // in order to build the correct url
            states['dataset'] = dataset;

            // Remove 'static' attribute in order to avoid displaying the search window
            delete states['static'];

            // Add day detail pop up
            states['day'] = data['data']['result']['date'];

            // Build a tmp url
            var tmpUrl = url + iam.url.createQueryString(states);

            if(index > 0) {
                html += ', ';
            }

            var tooltip = '';

            if(typeof iam.data['datasets'][dataset] !== 'undefined') {
                tooltip = 'Dataset type is ' + iam.data['datasets'][dataset]['type'];
            }

            html += '<a href="' + tmpUrl + '"' + (tooltip.length > 0 ? ' title="' + tooltip + '" data-toggle="tooltip" data-placement="bottom"' : '') + '>' + dataset + '</a>';
        });

        html += '<br/><small class="text-muted"><strong>Note:</strong> The list doesn\'t necessarily mean that the run is already processed in the displayed datasets</small>';
    } else {
        html += 'No datasets found';
    }

    html += '</td>';
    html += '</tr>';

    if(eventIdSearch && data['data']['result']['successfully']) {
        html += '<tr>';
        html += '<td>';
        html += '<strong>Sub Run</strong>';
        html += '</td>';
        html += '<td>';
        html += (typeof data['data']['result']['sub_run'] !== 'undefined' ? data['data']['result']['sub_run'] : 'not found');
        html += '</td>';
        html += '</tr>';
        html += '<tr>';
        html += '<td>';
        html += '<strong>File</strong> <span class="label label-info">L2</span>';
        html += '</td>';
        html += '<td>';
        html += '<code>' + (typeof data['data']['result']['file'] !== 'undefined' ? data['data']['result']['file'] : 'not found') + '</code>';
        html += '</td>';
        html += '</tr>';
    } else if(typeof data['data']['result']['paths'] !== 'undefined' && data['data']['result']['successfully']) {
        data['data']['result']['paths'].forEach(function(path) {
            html += '<tr>';
            html += '<td colspan="2">';
            html += '<strong>Path for dataset ' + path['dataset_id'] + '</strong>';

            if(typeof iam.data['datasets'][path['dataset_id']] !== 'undefined') {
                html += ' <span class="label label-info">' + iam.data['datasets'][path['dataset_id']]['type'] + '</span>';
            }

            html += '</td>';
            html += '</tr>';
            html += '<tr>';
            html += '<td colspan="2"><code>';
            html += path['path'];
            html += '</code></td>';
            html += '</tr>';
        });

        if(data['data']['result']['paths'].length > 0) {
            html += '<tr>';
            html += '<td colspan="2">';
            html += '<small class="text-muted"><strong>Note:</strong> The list of paths can be a subset of the list of datasets. Paths are only shown if the run has already been processed.</small>';
            html += '</td>';
            html += '</tr>';
        }
    }

    html += '</tbody>';
    html += '</table>';

    this.resultBox.append(html);
    $('[data-toggle="tooltip"]', this.resultBox).tooltip();
}

JobMonitorSearch.prototype._startSearching = function() {
    this.submitButton.prop('disabled', true);
    this.resultBox.html('');
    this.searchingIndicator.show();
}

JobMonitorSearch.prototype._endSearching = function() {
    this.submitButton.prop('disabled', false);
    this.searchingIndicator.hide();
}

JobMonitorSearch.prototype._noResult = function(message) {
    this.resultBox.html('<div class="alert alert-warning" role="alert"><strong>Nothing found:</strong> ' + message + '</div>');
}

JobMonitorSearch.prototype._somethingFound = function(message) {
    this.resultBox.html('<div class="alert alert-success" role="alert"><strong>Success:</strong> ' + message + '</div>');
}

JobMonitorSearch.prototype._error = function(message) {
    this.resultBox.html('<div class="alert alert-danger" role="alert"><strong>Error:</strong> ' + message + '</div>');
}

JobMonitorSearch.prototype.updateData = function(data) {
    this.data = data;
}

