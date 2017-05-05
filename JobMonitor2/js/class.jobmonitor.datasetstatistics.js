
function JobMonitorDatasetStatistics(queryUrl, main, get_selected_dataset) {
    this.get_selected_dataset = get_selected_dataset;
    this.loading = false;
    this.queryUrl = queryUrl;
    this.main = main;
    this.datasetId = undefined;
    this.dataLoaded = false;
    this.runData = undefined;

    var horizonalLinePlugin = {
      afterDraw: function(chartInstance) {
        var yScale = chartInstance.scales["y-axis-0"];
        var canvas = chartInstance.chart;
        var ctx = canvas.ctx;
        var index;
        var line;
        var style;
    
        if (chartInstance.options.horizontalLine) {
          for (index = 0; index < chartInstance.options.horizontalLine.length; index++) {
            line = chartInstance.options.horizontalLine[index];
    
            if (!line.style) {
              style = "rgba(169,169,169, .6)";
            } else {
              style = line.style;
            }
    
            if (line.y) {
              yValue = yScale.getPixelForValue(line.y);
            } else {
              yValue = 0;
            }
    
            ctx.lineWidth = 3;
    
            if (yValue) {
              ctx.beginPath();
              ctx.moveTo(0, yValue);
              ctx.lineTo(canvas.width, yValue);
              ctx.strokeStyle = style;
              ctx.stroke();
            }
    
            if (line.text) {
              ctx.fillStyle = style;
              ctx.fillText(line.text, 0, yValue + ctx.lineWidth);
            }
          }
          return;
        };
      }
    };

    Chart.pluginService.register(horizonalLinePlugin);

}

JobMonitorDatasetStatistics.prototype = new JobMonitorView('dataset-statistics');
JobMonitorDatasetStatistics.prototype.constructor = JobMonitorDatasetStatistics;

JobMonitorDatasetStatistics.prototype.updateView = function(data) {
    this.datasetId = this.get_selected_dataset();
    this.dataLoaded = false;
    this.runData = data;

    if(this.isDisplayed()) {
        this._queryData();
    }
}

JobMonitorDatasetStatistics.prototype.show = function() {
    JobMonitorView.prototype.show.call(this);

    if(!this.dataLoaded && typeof this.datasetId !== 'undefined') {
        this._queryData();
    }
}

JobMonitorDatasetStatistics.prototype.endLoading = function() {
    if(!this.loading) {
        JobMonitorView.prototype.endLoading.call(this);
    }
}

JobMonitorDatasetStatistics.prototype._queryData = function() {
    if(this.loading) {
        return;
    }

    var iam = this;
    this.loading = true;

    this.startLoading();

    var iam = this;
    var params = {'dataset': iam.datasetId, 'statistics': 1};

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

JobMonitorDatasetStatistics.prototype._error = function(msg) {
    this.getContent().html('<div id="jm-loading-error-customized" class="alert alert-danger" style="display: block;" role="alert">' +
                            '<strong>Error:</strong>' +
                            '<span>' + msg + '</span>' +
                            '</div>');
}

JobMonitorDatasetStatistics.prototype._chartTooltip = function(tooltipItem, data) {
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

JobMonitorDatasetStatistics.prototype._chartGetColor = function() {
    return 'rgb(' + Math.floor((Math.random() * 255) + 1) + ', ' + Math.floor((Math.random() * 255) + 1) + ', ' + Math.floor((Math.random() * 255) + 1) + ')';
}

JobMonitorDatasetStatistics.prototype._chartGetColorList = function(length) {
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

JobMonitorDatasetStatistics.prototype._generateChartExecutionTimeAndJobsPerHost = function(data) {
    var sortedKeysOfSites = Object.keys(data['data']['statistics']['execution_time']).sort(function(a,b) {
        return data['data']['statistics']['execution_time'][b]['jobs'] - data['data']['statistics']['execution_time'][a]['jobs'];
    });

    var execchartdata = {
        'data': [],
        'labels': [],
        'colors': undefined,
        'jobs': [],
        'mean': 0,
        'total_jobs': 0
    };

    $.each(sortedKeysOfSites, function(index, host) {
        var value = data['data']['statistics']['execution_time'][host];

        execchartdata['data'].push(value['exec_average']);
        execchartdata['labels'].push(host);
        execchartdata['total_jobs'] += value['jobs'];
        execchartdata['mean'] += value['jobs'] * value['exec_average'];
        execchartdata['jobs'].push(value['jobs']);
    });

    execchartdata['mean'] /= execchartdata['total_jobs'];

    execchartdata['colors'] = this._chartGetColorList(execchartdata['data'].length);

    var exec_chart_ctx = $('#exec-chart');
    var exec_chart = new Chart(exec_chart_ctx, {
        type: 'bar',
        data: {
            labels: execchartdata['labels'],
            datasets: [{
                type: 'bar',
                backgroundColor: execchartdata['colors'],
                data: execchartdata['data']
            }]
        },
        options: {
            tooltips: {
                callbacks: {
                    label: function(tooltipItems, data) {return 'Average: ' + Math.round(tooltipItems.yLabel * 100) / 100 + ' (Jobs: ' + execchartdata['jobs'][tooltipItems.index] + ')';}
                }
            },
            horizontalLine: [{
                y: execchartdata['mean'],
                style: this._chartGetColorList(execchartdata['data'].length + 1)[execchartdata['data'].length],
                text: 'Average'
            }],
            responsive: true,
            title: {
                display: true,
                text: 'Average Job Execution Time'
            },
            legend: {
                position: 'left',
                display: false
            },
            scales: {
                yAxes: [{
                    scaleLabel: {
                        display: true,
                        labelString: 'Processing Time in Seconds'
                    }
                }]
            }
        }
    });
    // ===============================================================
    var jobs_chart_ctx = $('#jobs-chart');
    var jobs_chart = new Chart(jobs_chart_ctx, {
        type: 'bar',
        data: {
            labels: execchartdata['labels'],
            datasets: [{
                type: 'bar',
                backgroundColor: execchartdata['colors'],
                data: execchartdata['jobs']
            }]
        },
        options: {
            responsive: true,
            title: {
                display: true,
                text: 'Number of Jobs per Host'
            },
            legend: {
                position: 'left',
                display: false
            },
            scales: {
                yAxes: [{
                    scaleLabel: {
                        display: true,
                        labelString: 'Number of Jobs'
                    }
                }]
            }
        }
    });
}

JobMonitorDatasetStatistics.prototype._generateChartJobsPerDay = function(data) {
    var jobsperdaychartdata = {
        'labels': [],
        'datasets': [],
        'dates': Object.keys(data['data']['statistics']['job_completion']).sort(),
        'grid_dataset_mapping': {},
        'date_index_mapping': {}
    };

    var addDays = function(date, days) {
        var dat = new Date(date.valueOf())
        dat.setDate(dat.getDate() + days);
        return dat;
    };

    var getDates = function(startDate, stopDate) {
        var dateArray = new Array();
        var currentDate = startDate;
        while (currentDate <= stopDate) {
            var d = new Date(currentDate);
            dateArray.push('' + d.getFullYear() + '-' + ((d.getMonth() + 1) < 10 ? '0' : '') + (d.getMonth() + 1) + '-' + (d.getDate() < 10 ? '0' : '') + d.getDate())
            currentDate = addDays(currentDate, 1);
        }
        return dateArray;
    };

    if(jobsperdaychartdata['dates'].length) {
        var first_date = jobsperdaychartdata['dates'][0].split('-');
        var last_date = jobsperdaychartdata['dates'][jobsperdaychartdata['dates'].length - 1].split('-');

        jobsperdaychartdata['labels'] = getDates(new Date(first_date[0], first_date[1] - 1, first_date[2], 0, 0, 0, 0), new Date(last_date[0], last_date[1] - 1, last_date[2], 0, 0, 0, 0));
    }

    $.each(jobsperdaychartdata['labels'], function(index, date) {
        jobsperdaychartdata['date_index_mapping'][date] = index;
    });

    $.each(data['data']['grid'], function(index, value) {
        jobsperdaychartdata['datasets'].push({'label': value['name'], 'backgroundColor': undefined, 'data': Array.apply(null, Array(jobsperdaychartdata['labels'].length)).map(Number.prototype.valueOf,0)});
        jobsperdaychartdata['grid_dataset_mapping'][value['name']] = jobsperdaychartdata['datasets'].length - 1;
    });

    $.each(data['data']['statistics']['job_completion'], function(index, grids) {
        for(var i = 0; i < grids.length; ++i) {
            var value = grids[i];

            jobsperdaychartdata['datasets'][jobsperdaychartdata['grid_dataset_mapping'][value['grid']]]['data'][jobsperdaychartdata['date_index_mapping'][index]] = value['jobs'];
        }
    });

    for(var i = 0; i < jobsperdaychartdata['datasets'].length; ++i) {
        jobsperdaychartdata['datasets'][i]['backgroundColor'] = this._chartGetColorList(jobsperdaychartdata['datasets'].length)[i];
    }

    // If there are more than one grid, add a line chart with the total number of jobs
    if(data['data']['grid'].length > 1) {
        var total_data = []
        jobsperdaychartdata['datasets'].forEach(function(value) {
            value['data'].forEach(function(entry, i) {
                if(typeof total_data[i] === 'undefined') {
                    total_data.push(entry);
                } else {
                    total_data[i] += entry;
                }
            });
        });

        var lineColor = this._chartGetColor();

        jobsperdaychartdata['datasets'].push({
            'label': 'Total Number of Jobs',
            'type': 'line',
            'lineTension': 0,
            'fill': false,
            'data': total_data,
            'backgroundColor': 'rgb(255, 255, 255)',
            'pointBackgroundColor': 'rgb(255, 255, 255)',
            'pointBorderColor': lineColor,
            'borderColor': lineColor
        });
    }

    var jobs_per_day_chart_ctx = $('#jobs-per-day-chart');
    var jobs_per_day_chart = new Chart(jobs_per_day_chart_ctx, {
        type: 'bar',
        data: {
            datasets: jobsperdaychartdata['datasets'],
            labels: jobsperdaychartdata['labels']
        },
        options: {
            scales: {
                yAxes: [{
                    /*type: 'logarithmic',*/
                    scaleLabel: {
                        display: true,
                        labelString: 'Number of Jobs'
                    }
                }]
            },
            responsive: true,
            title: {
                text: 'Job Completion per Day',
                display: true
            }
        }
    });
}

JobMonitorDatasetStatistics.prototype._generateChartSourceDatasetDelay = function(data) {
    if(typeof data['data']['statistics']['source_dataset_completion_delay'] === 'undefined' || data['data']['statistics']['source_dataset_completion_delay'].length == 0) {
        return;
    }

    var o = data['data']['statistics']['source_dataset_completion_delay'];
    var values = Object.values(o);
    values = values.filter(function(n) {return n >= 0}).map(function(n) {return n / 3600 / 24;});

    var min_val = Math.floor(Math.min(...values));

    var unit = 'Days';

    // Rescale if delay is large
    if(min_val > 90) {
        values = Object.values(o);
        values = values.filter(function(n) {return n >= 0}).map(function(n) {return n / 3600 / 24 / 30;});

        min_val = Math.floor(Math.min(...values));

        unit = 'Months';
    } else if(min_val > 30) {
        values = Object.values(o);
        values = values.filter(function(n) {return n >= 0}).map(function(n) {return n / 3600 / 24 / 7;});

        min_val = Math.floor(Math.min(...values));

        unit = 'Weeks';
    }

    var max_val = Math.floor(Math.max(...values));

    var delay_chart_data = {'label': [], 'data': []};

    for(var i = min_val; i <= max_val; ++i) {
        delay_chart_data['label'].push(i);
        delay_chart_data['data'].push(0);
    }

    values.forEach(function(v) {
        var e = Math.floor(v);
        var i = e - min_val;

        ++delay_chart_data['data'][i];
    });

    var delay_chart_ctx = $('#source-delay-chart');
    var delay_chart = new Chart(delay_chart_ctx, {
        type: 'bar',
        data: {
            labels: delay_chart_data['label'],
            datasets: [{
                backgroundColor: this._chartGetColor(),
                data: delay_chart_data['data']
            }]
        },
        options: {
            tooltips: {
                callbacks: {
                    title : function(tooltipItem, data) {
                        console.log(tooltipItem);
                        console.log(data);
                        return 'Delay of ' + tooltipItem[0].xLabel + ' ' + unit;
                    },
                    label : function(tooltipItem, data) {
                        return tooltipItem.yLabel + ' runs';
                    }
                }
            },
            legend: {
                display: false
            },
            scales: {
                yAxes: [{
                    scaleLabel: {
                        display: true,
                        labelString: 'Number of Runs'
                    }
                }],
                xAxes: [{
                    scaleLabel: {
                        display: true,
                        labelString: 'Delay in ' + unit
                    }
                }]
            },
            responsive: true,
            title: {
                display: true,
                text: 'Run Completion After Source Dataset Run Completion'
            }
        }
    });
}

JobMonitorDatasetStatistics.prototype._generateCharts = function(data) {
    this._generateChartExecutionTimeAndJobsPerHost(data);
    this._generateChartJobsPerDay(data);
    this._generateChartSourceDatasetDelay(data);
}

JobMonitorDatasetStatistics.prototype._queryComplete = function(data) {
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

    var content = '';

    content += '<div class="row">';
    content += '<div class="col-md-12"><canvas id="exec-chart"></canvas></div>';
    content += '</div>';

    content += '<div class="row">';
    content += '<div class="col-md-12"><canvas id="jobs-chart"></canvas></div>';
    content += '</div>';

    content += '<div class="row">';
    content += '<div class="col-md-12"><canvas id="jobs-per-day-chart"></canvas></div>';
    content += '</div>';

    content += '<div class="row">';
    content += '<div class="col-md-12"><canvas id="source-delay-chart"></canvas></div>';
    content += '</div>';

    this.getContent().html(content);
    this._generateCharts(data);

    this.dataLoaded = true;
}

JobMonitorDatasetStatistics.prototype.formatBytes = function(bytes, decimals) {
   if(bytes == 0) return '0 Bytes';
   var k = 1000,
       dm = decimals + 1 || 3,
       sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'],
       i = Math.floor(Math.log(bytes) / Math.log(k));
   return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}
