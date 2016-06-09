
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

    this.data = undefined;

    this.url = new JobMonitorLocation();
    this.url.init();

    this.viewOptions = new JobMonitorViews();

    this.datasets = new JobMonitorDatasets(function() {iam.updater.update(true);}, this.url, function(callback) {iam.updater.setNextAction(callback);});

    this.updater = new JobMonitorUpdater('query.php', this.url,
        function(data) {iam._updateData(data);},
        function() {iam._startLoading();},
        function() {iam._endLoading();},
        function() {iam._loadingError();},
        function() {return iam.datasets.getSelectedDataset();}
    );

    this.views = {
        'calendarView': new JobMonitorCalendar(this.url),
        'jobsView': new JobMonitorJobs()
    };

    this.staticPages = {
        'api': $('#jm-dialog-api'),
        'feedback': $('#jm-dialog-feedback'),
        'version': $('#jm-dialog-version')
    };

    this._staticContent();
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
    var showWarning = false;
    if(typeof this.data !== 'undefined') {
        $.each(this.data['data']['datasets'], function(dataset_id, value) {
            if(value['selected'] && value['season'] < 2015 && value['type'] === 'L3') {
                showWarning = true;
                return;
            }
        });
    }

    if(showWarning) {
        iam._showL3SeasonWarning();
    } else {
        iam._hideL3SeasonWarning();
    }
}

JobMonitor.prototype._updateData = function(data) {
    data = typeof data === 'undefined' ? {} : data;

    if(Object.keys(data).length === 0 ||
        typeof data['error'] === 'undefined' ||
        typeof data['error_msg'] === 'undefined' ||
        typeof data['data'] === 'undefined' ||
        typeof data['data']['runs'] === 'undefined' ||
        typeof data['data']['datasets'] === 'undefined') {
        // TODO: Better handling
        alert("Bad data");
        console.log(data);
        return;
    }

    // First check passed :)
    // Check of api version compatible
    if(!this.checkAPIVersionCompatibility(data['api_version'])) {
        alert('API version incompatible');
        console.log(data);
        return;
    }

    // Update dataset list
    this.data = data;

    if(typeof data['data']['datasets'] !== 'undefined') {
        this.datasets.update(data['data']['datasets']);
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
    }

    $('select').filter(function() {return !$(this).hasClass('selectpicker');}).addClass('selectpicker').selectpicker();
}    

JobMonitor.prototype._loadingError = function() {
    $('#jm-loading-error').show('slow');
}

JobMonitor.prototype._showL3SeasonWarning = function() {
    $('#jm-l3-pre-2015-season-note').show('slow');
}

JobMonitor.prototype._hideL3SeasonWarning = function() {
    $('#jm-l3-pre-2015-season-note').hide('slow');
}

JobMonitor.prototype.init = function () {
    this.updater.init();
    this.datasets.init();
    this.viewOptions.init(this.views);

    $.each(this.views, function(name, view) {
        view.init();
    });

    this.updater.update(false, true);
}

