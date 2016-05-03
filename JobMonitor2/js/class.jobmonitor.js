
/**
 * Creates an instance of JobMonitor.
 *
 * @this {JobMonitor}
 */
function JobMonitor(params) {
    params = typeof params !== 'undefined' ? params : {};

    /** @private */ this.legends = {'Processed': 'day-ok',
                                    'Processed w/ Errors': 'day-error',
                                    'Processing': 'day-proc',
                                    'Processing w/ Errors': 'day-proc-error',
                                    'Not All Runs Submitted Yet': 'day-not-all-submitted',
                                    'Not Validated Yet': 'day-not-validated'};

    this.updater = new JobMonitorUpdater('query.php', this._updateData, this._startLoading, this._endLoading, this._loadingError, this.getDatasetId);

    this._init();
}

JobMonitor.prototype._startLoading = function() {

}

JobMonitor.prototype._endLoading = function() {

}

JobMonitor.prototype._updateData = function(data) {

}

JobMonitor.prototype._loadingError = function() {

}

JobMonitor.prototype.getDatasetId = function() {
    return 1883;
}

JobMonitor.prototype._init = function () {
    //this.updater.update(false);
}

