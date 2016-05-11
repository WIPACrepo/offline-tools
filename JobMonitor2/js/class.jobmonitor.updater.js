
function JobMonitorUpdater(url, updateDataCallback, startLoadingCallback, endLoadingCallback, errorCallback, getDatasetIdCallback) {
    /** @private */ this.updateIntervals = [1, 5, 10, 20, 30, 60, 90, 120, -1];
    /** @private */ this.defaultInterval = -1;
    /** @private */ this.updateIntervalUnits = {'d': ' day', 'h': ' hour', 'm': ' min', 's': ' sec'};

    /** @private */ this.intervalSelection = $('#update-interval-selection');
    /** @private */ this.currentInterval = $('#current-interval', this.intervalSelection);
    /** @private */ this.forceUpdate = $('#force-update');

    /** @private */ this.lastUpdate = undefined;
    /** @private */ this.lastUpdateView = $('#last-update-view');

    /** @private */ this.lastUpdateTimer = undefined;
    /** @private */ this.updateTimer = undefined;

    /** @private */ this.blockUpdates = false;

    /** @private */ this.url = url;

    /** @private */ this.updateDataCallback = updateDataCallback;
    /** @private */ this.startLoadingCallback = startLoadingCallback;
    /** @private */ this.endLoadingCallback = endLoadingCallback;
    /** @private */ this.errorCallback = errorCallback;
    /** @private */ this.getDatasetIdCallback = getDatasetIdCallback;
}

/**
 * Initializes the updater.
 */
JobMonitorUpdater.prototype.init = function() {
    var iam = this;
    var menu = $('.dropdown-menu', this.intervalSelection).empty();

    this.updateIntervals.forEach(function(n) {
        var text = '<a href="#">';

        if(n < 1) {
            $(menu).append('<li role="separator" class="divider"></li>');
            text += 'never';
        } else {
            text += iam.convertSecondsToString(n * 60, false, false, iam.updateIntervalUnits);
        }

        text += '</a>';

        $(menu).append($('<li></li>').data('value', n).html(text));
    });

    $('li', menu).click(function(e) {
        $(iam.currentInterval).html($('a', this).html()).data('value', $(this).data('value'));
        iam.update(false);
        e.preventDefault();
    });

    $(this.forceUpdate).click(function(e) {
        iam.update(true);
        e.preventDefault();
    });

    this.lastUpdateTimer = setInterval(function() {
        iam._updateLastUpdate()
    }, 1000 * 60);

    // Execute methods
    $('li', menu).each(function() {
        if($(this).data('value') === iam.defaultInterval) {
            $('a', this).click();
        }
    });
}

/**
 * Sets the new update interval. If `never` is selected, no new timeout is created.
 */
JobMonitorUpdater.prototype._set_update_interval = function(fromNow) {
    fromNow = typeof fromNow !== 'undefined' ? fromNow : false;

    var iam = this;

    console.log('_set_update_interval(fromNow = ' + fromNow + ') called');

    var interval = $(this.currentInterval).data('value');

    if(typeof this.updateTimer !== 'undefined') {
        clearTimeout(this.updateTimer);
        this.updateTimer = undefined;

        console.log('  clear timeout');
    }

    if(interval > 0) {
        var newInterval = interval * 60 * 1000;

        if(fromNow) {
            newInterval -= (new Date() - this.lastUpdate)
        }

        this.updateTimer = setTimeout(function() {
            iam.update(true);
        }, newInterval);

        console.log('  set timeout to ' + Math.round(newInterval / 60. / 1000) + ' min');
    }
}

/**
 * Updates the data. It doesn't update if:
 * * A update is currently running
 * * The time since the last update is too small
 * * The update interval is set to `never`
 *
 * You can force the update if you pass `true`.
 *
 * @param {bool} force If it is set to `true`, the update is forced. The default is `false`.
 * @param {bool} force If it is set to `true`, only the list of datasets is requested. The default is `false`.
 */
JobMonitorUpdater.prototype.update = function(force, datasets_only) {
    force = typeof force !== 'undefined' ? force : false;
    datasets_only = typeof datasets_only !== 'undefined' ? datasets_only : false;
    var iam = this;

    console.log('update(force = ' + force + ') called')

    // If an update is running, don't start another one
    if(this.blockUpdates) {
        console.log('  updates blocked');
        this._set_update_interval(true);
        return;
    }

    var interval = $(this.currentInterval).data('value');

    // Only update if it is forced or the interval is too large already
    if(!force && typeof this.lastUpdate !== 'undefined' && (((new Date()) - this.lastUpdate) / 1000. < interval * 60 || interval === -1)) {
        console.log('  update isn\'t forced or it isn\'t time to execute a new update');
        this._set_update_interval(true);
        return;
    }

    this.blockUpdates = true;

    this._set_update_interval();

    console.log('  force = ' + force + '; timeDiff = ' + (((new Date()) - this.lastUpdate) / 1000.) + 's; targetInterval = ' + ($(this.currentInterval).data('value') * 60 + 5) + 's');

    // Creating request
    this.startLoadingCallback();
    this._startLoading();

    var params = {'dataset_id': iam.getDatasetIdCallback()};
    if(typeof params['dataset_id'] === 'undefined') {
        params = {};
    }

    $.getJSON(iam.url, params, 
        function(data) {
            iam.updateDataCallback(data);
    })
    .fail(function() {
            iam.errorCallback();
    })
    .always(function() {
        iam.endLoadingCallback();
        iam._endLoading();

        iam.lastUpdate = new Date();
        iam._updateLastUpdate();

        iam.blockUpdates = false;
    });
}

JobMonitorUpdater.prototype.updateDatasetId = function() {
}

JobMonitorUpdater.prototype._startLoading = function() {
    $('.fa-refresh', this.forceUpdate).addClass('fa-spin');
}

JobMonitorUpdater.prototype._endLoading = function() {
    $('.fa-refresh', this.forceUpdate).removeClass('fa-spin');
}

/**
 * Updates the 'Last Update' time in the view.
 *
 * @private
 */
JobMonitorUpdater.prototype._updateLastUpdate = function() {
    if(this.lastUpdate == undefined) {
        return;
    }

    var diffText = '';
    var diff = (new Date()) - this.lastUpdate;

    // convert into minutes
    diff = Math.round(diff / 1000. / 60);

    if(diff < 1) {
        diffText = 'just this minute';
    } else {
        diffText = this.convertSecondsToString(diff * 60, false, false) + ' ago';
    }

    $(this.lastUpdateView).html('(' + diffText + ')')
                          .attr('data-toggle', 'tooltip')
                          .attr('title', this.lastUpdate.toLocaleString('en-US'))
                          .attr('data-original-title', this.lastUpdate.toLocaleString('en-US'))
                          .attr('data-placement', 'bottom')
                          .tooltip();
}

/**
 * Converts seconds into '#d ##h ##m ##s'.
 *
 * @param {int} seconds Time in seconds
 * @param {bool} leading_zero If true, it adds a leading zero to each number (except for days). Default is true.
 * @param {bool} show_seconds If true, it adds the seconds to the string. Otherwise, only days, hours and minutes. Default is true.
 * @param {object} units Here you can specify how the individual units should be represented. The attribute names `s` (seconds), `m` (minutes),
 *                       `h` (hours), and `d` (days) are required. Default is the same like the attribute names.
 *
 * @returns {string} The converted time.
 */
JobMonitorUpdater.prototype.convertSecondsToString = function(seconds, leading_zero, show_seconds, units) {
    show_seconds = typeof show_seconds === 'undefined' ? true : show_seconds;
    leading_zero = typeof leading_zero === 'undefined' ? true : leading_zero;
    units = typeof units === 'undefined' ? {'s': 's', 'm': 'm', 'h': 'h', 'd': 'd'} : units;

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
        str += days.toString() + units['d'] + ' ';
    }

    if(hours > 0) {
        str += hours.toString() + units['h'] + ' ';
    }

    if(minutes > 0) {
        str += minutes.toString() + units['m'] + ' ';
    }

    if(show_seconds) {
        str += seconds.toString() + units['s'];
    }

    return str.trim();
}
