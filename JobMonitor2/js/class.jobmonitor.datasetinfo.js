
function JobMonitorDatasetInformation(queryUrl, main, get_selected_dataset) {
    this.get_selected_dataset = get_selected_dataset;
    this.loading = false;
    this.queryUrl = queryUrl;
    this.main = main;
    this.datasetId = undefined;
    this.dataLoaded = false;
    this.runData = undefined;
}

JobMonitorDatasetInformation.prototype = new JobMonitorView('dataset-info');
JobMonitorDatasetInformation.prototype.constructor = JobMonitorDatasetInformation;

JobMonitorDatasetInformation.prototype.updateView = function(data) {
    this.datasetId = this.get_selected_dataset();
    this.dataLoaded = false;
    this.runData = data;

    if(this.isDisplayed()) {
        this._queryData();
    }
}

JobMonitorDatasetInformation.prototype.show = function() {
    JobMonitorView.prototype.show.call(this);

    if(this.visible && !this.dataLoaded && typeof this.datasetId !== 'undefined') {
        this._queryData();
    }
}

JobMonitorDatasetInformation.prototype.endLoading = function() {
    if(!this.loading) {
        JobMonitorView.prototype.endLoading.call(this);
    }
}

JobMonitorDatasetInformation.prototype._queryData = function() {
    if(this.loading) {
        return;
    }

    var iam = this;
    this.loading = true;

    this.startLoading();

    var iam = this;
    var params = {'dataset': iam.datasetId};

    $.getJSON(this.queryUrl, params, 
        function(data) {
            iam._queryComplete(data);
    })
    .fail(function() {
            iam._error('An error occurred.');
    })
    .always(function() {
        iam.loading = false;
        iam.endLoading();
    });
}

JobMonitorDatasetInformation.prototype._error = function(msg) {
    this.getContent().html('<div id="jm-loading-error-customized" class="alert alert-danger" style="display: block;" role="alert">' +
                            '<strong>Error:</strong>' +
                            '<span>' + msg + '</span>' +
                            '</div>');
}

JobMonitorDatasetInformation.prototype._chartTooltip = function(tooltipItem, data) {
    var allData = data.datasets[tooltipItem.datasetIndex].data;
    var tooltipLabel = data.labels[tooltipItem.index];
    var tooltipData = allData[tooltipItem.index];
    var total = 0;
    for (var i in allData) {
        total += allData[i];
    }   
    var tooltipPercentage = Math.round((tooltipData / total) * 100);
    return tooltipLabel + ': ' + tooltipData + ' (' + tooltipPercentage + '%)';
}

JobMonitorDatasetInformation.prototype._chartGetColor = function() {
    return 'rgb(' + Math.floor((Math.random() * 255) + 1) + ', ' + Math.floor((Math.random() * 255) + 1) + ', ' + Math.floor((Math.random() * 255) + 1) + ')';
}

JobMonitorDatasetInformation.prototype._chartGetColorList = function(length) {
    Math.seedrandom('42');
    var base = ['rgb(255, 99, 132)', 'rgb(255, 159, 64)', 'rgb(255, 205, 86)', 'rgb(75, 192, 192)', 'rgb(54, 162, 235)', 'rgb(153, 102, 255)', 'rgb(231,233,237)'];

    var result = [];

    for(var i = 0; i < length; ++i) {
        if(i >= base.length) {
            result.push(this._chartGetColor());
        } else {
            result.push(base[i]);
        }
    }

    return result;
}

JobMonitorDatasetInformation.prototype._generateChartJobsPerGrid = function(data) {
    var gridchartdata = {
        'data': [data['data']['number_of_jobs']],
        'labels': ['unassigned'],
        'colors': undefined
    };

    $.each(data['data']['grid'], function(index, value) {
        gridchartdata['data'].push(value['jobs']);
        gridchartdata['data'][0] -= value['jobs'];
        gridchartdata['labels'].push(value['name']);
    });

    gridchartdata['colors'] = this._chartGetColorList(gridchartdata['data'].length);

    var grid_chart_ctx = $('#grid-chart');
    var grid_chart = new Chart(grid_chart_ctx, {
        type: 'pie',
        data: {
            datasets:[{
                data: gridchartdata['data'],
                backgroundColor: gridchartdata['colors']
            }],
            labels: gridchartdata['labels']
        },
        options: {
            responsive: true,
            tooltips: {
                callbacks: {
                    label: this._chartTooltip
                }
            }
        }
    });
}

JobMonitorDatasetInformation.prototype._generateChartJobsPerStatus = function(data) {
    var rawData = {};

    $.each(this.runData['runs'], function(runId, value) {
        $.each(value['jobs_states'], function(name, jobs) {
            if(!(name in rawData) && jobs > 0) {
                rawData[name] = 0;
            }

            if(jobs > 0) {
                rawData[name] += jobs;
            }
        });
    });
    
    var sortedKeys = Object.keys(rawData).sort();

    var statuschartdata = {
        'label': sortedKeys,
        'data': [],
        'colors': this._chartGetColorList(sortedKeys.length)
    };

    sortedKeys.forEach(function(key) {
        statuschartdata['data'].push(rawData[key]);
    });

    var status_chart_ctx = $('#status-chart');
    var status_chart = new Chart(status_chart_ctx, {
        type: 'pie',
        data: {
            labels: statuschartdata['label'],
            datasets: [{
                backgroundColor: statuschartdata['colors'],
                data: statuschartdata['data']
            }]
        },
        options: {
            responsive: true,
            title: {
                display: true,
                text: 'Job Status'
            },
            tooltips: {
                callbacks: {
                    label: this._chartTooltip
                }
            }
        }
    });
}

JobMonitorDatasetInformation.prototype._generateCharts = function(data) {
    this._generateChartJobsPerGrid(data);
    this._generateChartJobsPerStatus(data);
}

JobMonitorDatasetInformation.prototype._queryComplete = function(data) {
    data = typeof data === 'undefined' ? {} : data;

    if(Object.keys(data).length === 0 ||
        typeof data['error'] === 'undefined' ||
        typeof data['error_msg'] === 'undefined' ||
        typeof data['error_trace'] === 'undefined' ||
        typeof data['api_version'] === 'undefined' ||
        typeof data['data'] === 'undefined') {
        this._error('Received a non-interpretable answer');
        console.log(data);
        return;
    }

    if(!this.main.checkAPIVersionCompatibility(data['api_version'])) {
        console.log(data);
        this._error('Client version is not compatible with data. Force a page reload and try again.');
        return;
    }

    if(data['error']) {
        this._error(data['error_msg']);
        console.log(data);
        return;
    }

    var datasetId = data['data']['dataset_id'];

    var content = '<div class="row">';
    //content += '<div class="col-md-4"><img src="http://grid.icecube.wisc.edu/filtering/graph/type/grid%20contribution/dataset/' + datasetId + '" /></div>';
    content += '<div class="col-md-4"><table class="table table-striped"><thead><th></th><th>Data</th><th>Files</th></thead><tbody>';
    content += '<tr><td>Input</td><td>' + this.formatBytes(data['data']['input']['size']) + '</td><td>' + data['data']['input']['files'] + '</td></tr>';
    content += '<tr><td>Output</td><td>' + this.formatBytes(data['data']['output']['size']) + '</td><td>' + data['data']['output']['files'] + '</td></tr>';
    content += '</tbody></table></div>';
    content += '<div class="col-md-4">' +
                '<table class="table table-striped">' +
                '<thead>' +
                '<th>Grid Name</td>' +
                '<th>Enabled</td>' +
                '<th>Jobs</td>' +
                '<th>Evictions</td>' +
                '<th>Failures</td>' +
                '</thead>' +
                '<tbody>';

    $.each(data['data']['grid'], function(index, value) {
        content += '<tr>';
        content += '<td>' + value['name'] + '</td>';
        content += '<td>' + (value['enabled'] ? '&#10003;' : '&#10007;' ) + '</td>';
        content += '<td>' + value['jobs'] + '</td>';
        content += '<td>' + value['evictions'] + '</td>';
        content += '<td>' + value['failures'] + '</td>';
        content += '</tr>';
    });

    content += '</tbody>' +
                '</table>' +
                '</div>';
    content += '<div class="col-md-4">';
    content += '<strong>Metaproject:</strong> ' + data['data']['metaproject'] + '<br/>';
    content += '<strong>Tarball(s):</strong><ul>';
    $.each(data['data']['tarball'], function(index, name) {
        content += '<li>' + name + '</li>';
    });
    content += '</ul>';
    content += '</div>';

    content += '</div>';

    content += '<div class="row">';
    content += '<div class="col-md-4"><canvas id="grid-chart"></canvas></div>';
    content += '<div class="col-md-4"><canvas id="status-chart"></canvas></div>';
    content += '<div class="col-md-4">';

    if(typeof data['data']['source_dataset_ids'] != 'undefined') {
        if(data['data']['source_dataset_ids'].length > 0) {
            content += '<strong>Source Datasets:</strong><ul>';

            data['data']['source_dataset_ids'].forEach(function(dataset_id) {
                content += '<li><a href="./?dataset=' + dataset_id + '" target="_blank">' + dataset_id + '</a></li>';
            });

            content += '</ul>';
        }
    }

    if(typeof data['data']['level3_information'] !== 'undefined') {
        if(data['data']['level3_information'] !== null && Object.keys(data['data']['level3_information']).length > 0) {
            content += '<strong>Level3 Configurations:</strong><ul>';

            $.each(data['data']['level3_information'], function(key, value) {
                content += '<li><strong>' + key + ':</strong> ' + value + '</li>';
            });

            content += '</ul>';
        }
    }

    content += '</div>';
    content += '</div>';

    this.getContent().html(content);
    this._generateCharts(data);

    this.dataLoaded = true;
}

JobMonitorDatasetInformation.prototype.formatBytes = function(bytes, decimals) {
   if(bytes == 0) return '0 Bytes';
   var k = 1000,
       dm = decimals + 1 || 3,
       sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'],
       i = Math.floor(Math.log(bytes) / Math.log(k));
   return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}
