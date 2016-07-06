
function JobMonitorSearch(url, findSeasonByRunIdCallback, searchUrl, apiCompatibilityCheckerCallback) {
    this.url = url;
    this.findSeasonByRunIdCallback = findSeasonByRunIdCallback;
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

    if(eventIdMode) {
        var eventId = this.inputEventId.val();
        this._searchRequest(runId, eventId);
    } else {
        this._startSearching();
        if(typeof this.data !== 'undefined' && runId in this.data['runs']) {
            this.resultBox.html('');
            this.modal.modal('hide');
            $('table td[data-jm-day="' + this.data['runs'][runId]['date'] + '"]', this.calendar).click();
        } else {
            var suggestedSeason = this.findSeasonByRunIdCallback(runId);

            this._noResult('Run ' + runId + ' could not be found within this dataset.'
                        + (suggestedSeason > -1 ? ' Run ' + runId + ' should be within season ' + suggestedSeason + '.' : '')
                        + ' You can also check <a href="https://live.icecube.wisc.edu/run/' + runId + '/" target="_blank">i3live</a>.'
                        + ' Consider that only good runs are displayed in the calendar view.');
        }
        this._endSearching();
    }
}

JobMonitorSearch.prototype._searchRequest = function(runId, eventId) {
    if(this.searching) {
        return;
    }

    this.searching = true;

    this._startSearching();

    var iam = this;
    var params = {'run_id': runId, 'event_id': eventId};

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
        return;
    }

    // Ok, we have a result. Let's build the result HTML...
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
    html += '<strong>Event Id</strong>';
    html += '</td>';
    html += '<td>';
    html += data['data']['query']['event_id'];
    html += '</td>';
    html += '</tr>';
    html += '<tr>';
    html += '<td>';
    html += '<strong>Sub Run</strong>';
    html += '</td>';
    html += '<td>';
    html += data['data']['result']['sub_run'];
    html += '</td>';
    html += '</tr>';
    html += '<tr>';
    html += '<td>';
    html += '<strong>File</strong>';
    html += '</td>';
    html += '<td>';
    html += '<code>' + data['data']['result']['file'] + '</code>';
    html += '</td>';
    html += '</tr>';
    html += '</tbody>';
    html += '</table>';

    this.resultBox.html(html);
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
    this.resultBox.html('<div class="alert alert-danger" role="alert"><strong>Nothing found:</strong> ' + message + '</div>');
}

JobMonitorSearch.prototype._error = function(message) {
    this.resultBox.html('<div class="alert alert-danger" role="alert"><strong>Error:</strong> ' + message + '</div>');
}

JobMonitorSearch.prototype.updateData = function(data) {
    this.data = data;
}

