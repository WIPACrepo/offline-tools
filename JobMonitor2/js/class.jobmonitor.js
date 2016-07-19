
/**
 * Creates an instance of JobMonitor.
 *
 * @this {JobMonitor}
 */
function JobMonitor(params) {
    var iam = this;

    params = typeof params !== 'undefined' ? params : {};

    this.apiVersion = $('#jm-api-version').html().split('.');
    try {
        for(var i = 0; i < this.apiVersion.length; ++i) {
            this.apiVersion[i] = parseInt(this.apiVersion[i]);
        }
    } catch(err) {
        console.log(err);
        alert('Cannot determine client API version.');
        return;
    }

    this.personnelData = $('#jm-personnel');
    this.personnel = {'name': this.personnelData.attr('data-jm-name'),
                      'email': this.personnelData.attr('data-jm-email'),
                      'slack_user': this.personnelData.attr('data-jm-slack-user'),
                      'slack_channel': this.personnelData.attr('data-jm-slack-channel'),};

    this.isTouchDevice = ("ontouchstart" in window) || window.DocumentTouch && document instanceof DocumentTouch;

    this.data = undefined;

    this.url = new JobMonitorLocation();
    this.url.init();

    this.viewOptions = new JobMonitorViews(this.url);

    this.datasets = new JobMonitorDatasets(function() {iam.updater.update(true);}, this.url, function(callback) {iam.updater.setNextAction(callback);}, this);

    this.search = new JobMonitorSearch(this.url, 'search.php', this);

    this.updater = new JobMonitorUpdater('query.php', this.url,
        function(data) {iam._updateData(data);},
        function() {iam._startLoading();},
        function() {iam._endLoading();},
        function() {iam._loadingError();},
        function() {return iam.datasets.getSelectedDataset();}
    );

    this.views = {
        'calendarView': new JobMonitorCalendar(this.url, this.isTouchDevice),
        'jobsView': new JobMonitorJobs(this.url)
    };

    this.staticPages = {
        'api': $('#jm-dialog-api'),
        'feedback': $('#jm-dialog-feedback'),
        'version': $('#jm-dialog-version'),
        'search': $('#jm-dialog-search')
    };

    this._staticContent();
}

JobMonitor.prototype.getPersonnelData = function(key) {
    return this.personnel[key];
}

JobMonitor.prototype.createLabelSeason = function(season, verbose) {
    verbose = typeof verbose !== 'undefined' ? verbose : false;

    var text = '';

    if(verbose) {
        text = 'Season ';
    }

    return '<span class="label label-info">' + text + season + '</span>';
}

JobMonitor.prototype.createLabelWorkingGroup = function(workingGroup, verbose) {
    verbose = typeof verbose !== 'undefined' ? verbose : false;

    var text = '';

    if(verbose) {
        text = 'Working Group: ';
    }

    return '<span class="label jm-label-working-group">' + text + workingGroup + '</span>';
}

JobMonitor.prototype.createLabelComment = function(comment, verbose) {
    verbose = typeof verbose !== 'undefined' ? verbose : false;

    var text = '';

    if(verbose) {
        text = 'Comment: ';
    }

    return '<span class="label jm-label-comment">' + text + comment + '</span>';
}

JobMonitor.prototype.createLabelDatasetType = function(type, verbose, customText) {
    verbose = typeof verbose !== 'undefined' ? verbose : false;
    customText = typeof customText !== 'undefined' ? customText : false;

    var colorClass = 'label-default';
    var text = '';

    if(verbose) {
        text = 'Type ';
    }

    type = type.toUpperCase();

    switch(type) {
        case 'L2':
            colorClass = 'label-success';
            break;

        case 'L3':
            colorClass = 'label-warning';
            break;
    }

    if(customText !== false) {
        text = customText;
        type = '';
    }

    return '<span class="label ' + colorClass + '">' + text + type + '</span>';
}

JobMonitor.prototype.findSeasonByRunId = function(runId) {
    var foundSeason = -1;

    $.each(this.data['data']['seasons'], function(year, season) {
        if((runId >= season['first_run'] && season['first_run'] != "-1") || $.inArray(runId, season['test_runs']) !== -1) {
            foundSeason = year;
        }

        if(runId < season['first'] && foundSeason > -1) {
            return;
        }
    });

    return foundSeason;
}

JobMonitor.prototype.findDatasetsBySeason = function(season) {
    var datasets = [];

    $.each(this.data['data']['datasets'], function(datasetId, dataset) {
        if(dataset['season'] == season) {
            datasets.push(datasetId);
        }
    });

    return datasets;
}

JobMonitor.prototype.checkAPIVersionCompatibility = function(dataAPIVersion) {
    try {
        dataAPIVersion = String(dataAPIVersion).split('.');

        for(var i = 0; i < dataAPIVersion.length; ++i) {
            dataAPIVersion[i] = parseInt(dataAPIVersion[i]);
        }

        if(this.apiVersion.length !== 2 || dataAPIVersion.length !== 2) {
            console.log('No valid version numbers');
            return false;
        }

        return dataAPIVersion[0] === this.apiVersion[0] && dataAPIVersion[1] >= this.apiVersion[1];
    } catch(err) {
        console.log(err);
        return false;
    }
}

JobMonitor.prototype._staticContent = function() {
    var iam = this;

    $.each(this.staticPages, function(name, obj) {
        obj.on('shown.bs.modal', function (e) {
            iam.url.setState('static', name);
            iam.url.pushState();
        });

        obj.on('hidden.bs.modal', function (e) {
            iam.url.removeState('static');
            iam.url.pushState();
        });
    });

    if(this.url.hasState('static')) {
        var page = this.url.getState('static');

        if($.inArray(page, Object.keys(this.staticPages)) !== -1) {
            this.staticPages[page].modal();
        } else {
            this.url.removeState('static');
            this.url.pushState();
        }
    }
}

JobMonitor.prototype._startLoading = function() {
    this.datasets.startLoading();
    this._loadingErrorHide();
    this.hideError();

    $.each(this.views, function(name, view) {
        view.startLoading();
    });
}

JobMonitor.prototype._endLoading = function() {
    var iam = this;

    this.datasets.endLoading();

    $.each(this.views, function(name, view) {
        view.endLoading();
    });

    // Check if L3 dataset before 2015
    var showL3Warning = false;
    if(typeof this.data !== 'undefined') {
        $.each(this.data['data']['datasets'], function(dataset_id, value) {
            if(value['selected'] && value['season'] < 2015 && value['type'] === 'L3') {
                showL3Warning = true;
                return;
            }
        });
    }

    if(showL3Warning) {
        iam._showL3SeasonWarning();
    } else {
        iam._hideL3SeasonWarning();
    }

    // Check if L2 dataset of season 2010 or 2012
    var showL2Warning = false;
    if(typeof this.data !== 'undefined') {
        $.each(this.data['data']['datasets'], function(dataset_id, value) {
            if(value['selected'] && (value['season'] == 2010 || value['season'] == 2012) && value['type'] === 'L2') {
                showL2Warning = true;
                return;
            }
        });
    }

    if(showL2Warning) {
        iam._showL2Season1012Warning();
    } else {
        iam._hideL2Season1012Warning();
    }
}

JobMonitor.prototype._updateData = function(data) {
    data = typeof data === 'undefined' ? {} : data;

    if(Object.keys(data).length === 0 ||
        typeof data['error'] === 'undefined' ||
        typeof data['error_msg'] === 'undefined' ||
        typeof data['error_trace'] === 'undefined' ||
        typeof data['api_version'] === 'undefined' ||
        typeof data['data'] === 'undefined' ||
        typeof data['data']['runs'] === 'undefined' ||
        typeof data['data']['datasets'] === 'undefined') {

        this.showError('Data looks unexpected.');

        console.log(data);
        return;
    }

    // First check passed :)
    // Check of api version compatible
    if(!this.checkAPIVersionCompatibility(data['api_version'])) {
        this.showError('API version incompatible');
        console.log(data);
        return;
    }

    // Update dataset list
    this.data = data;

    if(typeof data['data']['datasets'] !== 'undefined' && typeof data['data']['seasons'] !== 'undefined') {
        this.datasets.update(data['data']);
    }

    if(typeof this.datasets.getSelectedDataset() === 'undefined') {
        this.datasets.show();

        $.each(this.views, function(name, view) {
            view.hide();
        });
    } else {
        this.datasets.hide();

        $.each(this.views, function(name, view) {
            view.show();
            view.updateView(data['data']);
        });

        this.search.updateData(data['data']);
    }

    $('select').filter(function() {return !$(this).hasClass('selectpicker');}).addClass('selectpicker').selectpicker();
}    

JobMonitor.prototype._loadingError = function() {
    $('#jm-loading-error').show('slow');
}

JobMonitor.prototype._loadingErrorHide = function() {
    $('#jm-loading-error').hide('slow');
}

JobMonitor.prototype.showError = function(msg) {
    $('#jm-loading-error-customized span').html(msg);
    $('#jm-loading-error-customized').show('slow');
}

JobMonitor.prototype.hideError = function(msg) {
    $('#jm-loading-error-customized').hide('slow');
}

JobMonitor.prototype._showL3SeasonWarning = function() {
    $('#jm-l3-pre-2015-season-note').show('slow');
}

JobMonitor.prototype._hideL3SeasonWarning = function() {
    $('#jm-l3-pre-2015-season-note').hide('slow');
}

JobMonitor.prototype._showL2Season1012Warning = function() {
    $('#jm-l2-2010-2012-season-note').show('slow');
}

JobMonitor.prototype._hideL2Season1012Warning = function() {
    $('#jm-l2-2010-2012-season-note').hide('slow');
}

JobMonitor.prototype.init = function () {
    this.updater.init();
    this.datasets.init();
    this.search.init();

    $.each(this.views, function(name, view) {
        view.init();
    });

    this.viewOptions.init(this.views);
    this.updater.update(false, true);
}

