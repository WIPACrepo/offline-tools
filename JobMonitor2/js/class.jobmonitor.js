
/**
 * Creates an instance of JobMonitor.
 *
 * @this {JobMonitor}
 */
function JobMonitor(params) {
    var iam = this;

    params = typeof params !== 'undefined' ? params : {};

    /** @private */ this.legends = {'Processed': 'day-ok',
                                    'Processed w/ Errors': 'day-error',
                                    'Processing': 'day-proc',
                                    'Processing w/ Errors': 'day-proc-error',
                                    'Not All Runs Submitted Yet': 'day-not-all-submitted',
                                    'Not Validated Yet': 'day-not-validated'};

    this.datasets = new JobMonitorDatasets(function() {iam.updater.update(true);});

    this.updater = new JobMonitorUpdater('query.php',
        function(data) {iam._updateData(data);},
        function() {iam._startLoading();},
        function() {iam._endLoading();},
        function() {iam._loadingError();},
        function() {return iam.datasets.getSelectedDataset();}
    );

    console.log(this.datasets);
}

JobMonitor.prototype._startLoading = function() {
    this.datasets.startLoading();
}

JobMonitor.prototype._endLoading = function() {
    this.datasets.endLoading();
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
    // Update dataset list
    if(typeof data['data']['datasets'] !== 'undefined') {
        this.datasets.update(data['data']['datasets']);
    }
}    

JobMonitor.prototype._loadingError = function() {

}

JobMonitor.prototype.init = function () {
    this.updater.init();
}

