
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
        function(data) { iam._updateData(data);},
        function() {iam._startLoading();},
        function() {iam._endLoading();},
        function() {iam._loadingError();},
        function() {return iam.datasets.getSelectedDataset();}
    );

    this.views = {
        'calendarView': new JobMonitorCalendar(this.url, this.isTouchDevice),
        'jobsView': new JobMonitorJobs(this.url),
        'datasetInfoView': new JobMonitorDatasetInformation('dataset.php', this, function() {return iam.datasets.getSelectedDataset();}),
        'datasetStatisticsView': new JobMonitorDatasetStatistics('dataset.php', this, function() {return iam.datasets.getSelectedDataset();})
    };

    this.staticPages = {
        'api': '#jm-dialog-api',
        'feedback': '#jm-dialog-feedback',
        'version': '#jm-dialog-version',
        'search': '#jm-dialog-search',
        'testruns': '#jm-dialog-24h-test-runs',
        'pass2-lost-files': {'modal': '#jm-dialog-pass2-lolf', 'onopen': function() {iam._initPass2ListOfLostFiles();}}
    };

    this._staticContent();
}

JobMonitor.prototype.formatNumber = function(n, ) {
    return n.toLocaleString('en-US',  {maximumFractionDigits: 0});
}

JobMonitor.prototype._initPass2ListOfLostFiles = function() {
    var iam = this;

    if($.fn.DataTable.isDataTable('#jm-dialog-pass2-lolf-table')) {
        $('#jm-dialog-pass2-lolf-table').DataTable().destroy();
    }

    var pass2lolf = $('#jm-dialog-pass2-lolf-table').DataTable({
        "ajax": "pass2-lost-files.php?datatables=true",
        "columns": [
            {"data": "run_id"},
            {"data": "sub_run"},
            {"data": "season"},
            {"data": "type"},
            {"data": "path", "className": "jm-shorten-text cursor"},
            {"data": "livetime"},
            {"data": "last_change"},
            {"data": "comment"},
            {"data": "resolved"}
        ],
        "columnDefs": [
            {
                "targets": [8],
                "visible": false,
                "searchable": false
            },
            {
                "targets": 7,
                "render": function(data, type, row, meta) {
                    if(data !== null && data != '') {
                        return data.replace(/\#(\d+)/g, '<a href="https://tracker.icecube.wisc.edu/Ticket/Display.html?id=$1" target="_blank">#$1</a>');
                    } else {
                        return '';
                    }
                }
            },
            {
                "targets": 5,
                "render": function(data, type, row, meta) {
                    if(data !== null) {
                        return '' + data + 's';
                    } else {
                        return 'N/A';
                    }
                }
            }
        ],
        "createdRow": function(row, data, index) {
            if(data['resolved'] == '1') {
                $(row).addClass('jm-pass2-lolf-resolved');
            }
        },
        "footerCallback": function(row, data, start, end, display) {
            var api = this.api(), data;

            var count_total = 0;
            var count_total_page = 0;
            var count_not_available = 0;
            var count_not_available_page = 0;
 
            var intVal = function(i) {
                return typeof i === 'string' ?
                    i.replace(/[\$,]/g, '')*1 :
                    typeof i === 'number' ?
                        i : 0;
            };
 
            total = api
                .column(5)
                .data()
                .reduce( function (a, b) {
                    ++count_total;
                    var ia = intVal(a);
                    var ib = intVal(b);

                    if(ia == 0) {
                        ++count_not_available;
                    }

                    if(ib == 0) {
                        ++count_not_available;
                    }

                    return ia + ib;
                }, 0);
 
            pageTotal = api
                .column(5, {page: 'current'})
                .data()
                .reduce( function (a, b) {
                    ++count_total_page;

                    var ia = intVal(a);
                    var ib = intVal(b);

                    if(a == null) {
                        ++count_not_available_page;
                    }

                    if(b == null) {
                        ++count_not_available_page;
                    }

                    return ia + ib;
                }, 0);
 
            var approxPage = count_not_available_page > 0 ? total / (count_total - count_not_available) * count_not_available_page : 0;
            var approxTotal = count_not_available > 0 ? total / (count_total - count_not_available) * count_not_available : 0;

            $(api.column(5).footer()).html(
                '' + iam.formatNumber(pageTotal) +'s ' + (approxPage > 0 ? ' + &asymp;' + iam.formatNumber(approxPage) + 's' : '') + '<br>' +
                '('+ iam.formatNumber(total) +'s' + (approxTotal > 0 ? ' + &asymp;' + iam.formatNumber(approxTotal) + 's' : '') + ' total)'
            );
        }
    });

    pass2lolf.on('draw', function() {
        $('#jm-dialog-pass2-lolf-table tbody td.jm-shorten-text').each(function() {
            $(this).popover({
                'trigger': 'click',
                'container': 'body',
                'placement': 'bottom',
                'html': true,
                'title': 'File path',
                'content': function() {
                    var path = $(this).html();
                    return '<pre class="jm-path-display">' + path + '</pre>';
                }
            });
        });

        // Close popover on click outside of popover
        $('body').on('click', function(e) {
            $('#jm-dialog-pass2-lolf-table tbody td.jm-shorten-text').each(function () {
                if (!$(this).is(e.target) && $(this).has(e.target).length === 0 && $('.popover').has(e.target).length === 0) {
                    $(this).popover('hide');
                }
            });
        });
    });
}

JobMonitor.prototype.getPersonnelData = function(key) {
    return this.personnel[key];
}

JobMonitor.prototype.createLabelDatasetStatus = function(s, verbose) {
    verbose = typeof verbose !== 'undefined' ? verbose : false;

    var states = {
        'PREPARATION': '<span class="glyphicon glyphicon-asterisk" aria-hidden="true" data-toggle="tooltip" title="{TOOLTIP}" data-placement="bottom"></span>',
        'PROCESSING': '<span class="glyphicon glyphicon-cog" aria-hidden="true" data-toggle="tooltip" title="{TOOLTIP}" data-placement="bottom"></span>',
        'COMPLETE': '<span class="glyphicon glyphicon-ok-circle" aria-hidden="true" data-toggle="tooltip" title="{TOOLTIP}" data-placement="bottom"></span>',
        'FAILED': '<span class="glyphicon glyphicon-remove-circle" aria-hidden="true" data-toggle="tooltip" title="{TOOLTIP}" data-placement="bottom"></span>',
        'TEST': '<span class="glyphicon glyphicon-ban-circle" aria-hidden="true" data-toggle="tooltip" title="{TOOLTIP}" data-placement="bottom"></span>',
        'SUSPENDED': '<span class="glyphicon glyphicon-hourglass" aria-hidden="true" data-toggle="tooltip" title="{TOOLTIP}" data-placement="bottom"></span>'
    };

    var text = s['name'];
 
    if(typeof s['comment'] !== undefined) {
        if(s['comment'] !== null && s['comment'].length > 0) {
            text += ': ' + s['comment'];
        }
    }

    if(verbose) {
        return '<span class="label label-default" data-toggle="tooltip" title="Last status update: ' + s['date'] + '" data-placement="bottom">' + text + '</span>';
    } else {
        if(s['name'] in states) {
            return states[s['name']].replace('{TOOLTIP}', text);
        } else {
            return '<span class="glyphicon glyphicon-question-sign" aria-hidden="true" data-toggle="tooltip" title="{TOOLTIP}" data-placement="bottom"></span>'.replace('{TOOLTIP}', text);
        }
    }
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
        var onopen = undefined;

        if(typeof obj.modal !== 'undefined') {
            if(typeof obj.onopen !== 'undefined') {
                onopen = obj.onopen;
            }

            obj = $(obj.modal);
        } else if(typeof obj === 'string') {
            obj = $(obj);
        } else {
            alert('Misconfigured static page: ' + name);
        }

        obj.on('shown.bs.modal', function (e) {
            iam.url.setState('static', name);
            iam.url.pushState();

            if(typeof onopen !== 'undefined') {
                onopen();
            }
        });

        obj.on('hidden.bs.modal', function (e) {
            iam.url.removeState('static');
            iam.url.pushState();
        });
    });

    if(this.url.hasState('static')) {
        var page = this.url.getState('static');

        if($.inArray(page, Object.keys(this.staticPages)) !== -1) {
            if(typeof this.staticPages[page] == 'string') {
                $(this.staticPages[page]).modal();
            } else if(typeof this.staticPages[page].modal !== 'undefined') {
                $(this.staticPages[page].modal).modal();
            } else {
                alert('Misconfigured static page: ' + page);
            }
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
            if(value['selected'] && dataset_id < 1885 && value['type'] === 'L3') {
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
            if(value['selected'] && (value['season'] == 2010 || value['season'] == 2012) && value['type'] === 'L2' && dataset_id <= 1872) {
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
    }

    // Search engine needs information about datasets
    this.search.updateData(data['data']);

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

