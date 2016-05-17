
/**
 * Creates an instance of JobMonitor.
 *
 * @this {JobMonitor}
 */
function JobMonitor(params) {
    var iam = this;

    params = typeof params !== 'undefined' ? params : {};

    this.apiVersion = parseInt($('#jm-api-version').html());

    this.datasets = new JobMonitorDatasets(function() {iam.updater.update(true);});

    this.updater = new JobMonitorUpdater('query.php',
        function(data) {iam._updateData(data);},
        function() {iam._startLoading();},
        function() {iam._endLoading();},
        function() {iam._loadingError();},
        function() {return iam.datasets.getSelectedDataset();}
    );

    this.views = {
        'calendarView': new JobMonitorCalendar(),
        'jobsView': new JobMonitorJobs()
    };
}

JobMonitor.prototype._startLoading = function() {
    this.datasets.startLoading();

    $.each(this.views, function(name, view) {
        view.startLoading();
    });
}

JobMonitor.prototype._endLoading = function() {
    this.datasets.endLoading();

    $.each(this.views, function(name, view) {
        view.endLoading();
    });
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
    if(this.apiVersion != data['api_version']) {
        alert('API version incompatible');
        console.log(data);
    }

    // Update dataset list
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
}    

JobMonitor.prototype._loadingError = function() {

}

JobMonitor.prototype.init = function () {
    this.updater.init();
    this.datasets.init();

    $.each(this.views, function(name, view) {
        view.init();
    });

    this.updater.update(false, true);
}

