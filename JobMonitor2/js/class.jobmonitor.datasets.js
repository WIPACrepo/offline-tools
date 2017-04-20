
function JobMonitorDatasets(updateCallback, url, nextActionCallback, main) {
    this.datasetList = $('#jm-dataset-dropdown');
    this.currentDataset = $('#current-dataset', this.datasetList);
    this.title = $('#jm-dataset-title');
    this.updateCallback = updateCallback;
    this.url = url;
    this.main = main;
    this.nextActionCallback = nextActionCallback;
}

JobMonitorDatasets.prototype = new JobMonitorView('dataset-selection');
JobMonitorDatasets.prototype.constructor = JobMonitorDatasets;

JobMonitorDatasets.prototype.update = function(data) {
    var iam = this; 

    var datasets = data['datasets'];
    var seasons = data['seasons'];

    var menu = $('.dropdown-menu', this.datasetList).empty();
    var selected = -1;

    // We want a sorted list (by season, dataset id)
    var keys = Object.keys(datasets);

    for(var i = 0; i < keys.length; ++i) {
        keys[i] = [keys[i], 0];

        if(typeof datasets[keys[i][0]]['season'] !== undefined && datasets[keys[i][0]]['season'] !== null) {
            keys[i][1] = datasets[keys[i][0]]['season'];
        }
    }

    keys.sort(function(a, b) {return a[1] - b[1];});
    keys.reverse();

    for(var i = 0; i < keys.length; ++i) {
        keys[i] = keys[i][0];
    }

    // DropDown
    var datasetTypes = []
    var orderedDatasets = {};
    
    $.each(seasons, function(season, value) {
        orderedDatasets[season] = {};

        $.each(datasets, function(datasetId, dataset) {
            if(dataset['supported'] && dataset['season'] == season) {
                if(typeof orderedDatasets[season][dataset['type']] === 'undefined') {
                    orderedDatasets[season][dataset['type']] = [];
                }

                orderedDatasets[season][dataset['type']].push(dataset);

                if(-1 == $.inArray(dataset['type'], datasetTypes)) {
                    datasetTypes.push(dataset['type']);
                }
            }
        });
    });

    var dropDownHtml = '<li><table class="table jm-dataset-list">';

    // Loop ordered over the ordereddatasets
    var seasonSorted = Object.keys(orderedDatasets);
    seasonSorted.sort();
    seasonSorted.reverse();

    datasetTypes.sort();

    seasonSorted.forEach(function(season) {
        var numOfLines = 1;

        var first = true;
        datasetTypes.forEach(function(type) {
            if(typeof orderedDatasets[season][type] !== 'undefined') {
                numOfLines += orderedDatasets[season][type].length;

                if(first) {
                    first = false;
                } else {
                    numOfLines += 1;
                }
            }
        });

        dropDownHtml += '<tr>';
        dropDownHtml += '<td rowspan="' + numOfLines + '"><strong>' + season + '</strong></td>';

        var first = true;

        datasetTypes.forEach(function(type) {
            if(typeof orderedDatasets[season][type] !== 'undefined') {
                if(first) {
                    first = false;
                    dropDownHtml += '<td></td>';
                } else {
                    dropDownHtml += '<tr>';
                    dropDownHtml += '<td class="jm-dataset-status"></td>';
                }

                dropDownHtml += '<td class="jm-dataset-type">';
                dropDownHtml += '<strong>' + type + ' Datasets</strong>';
                dropDownHtml += '</td>';
                dropDownHtml += '</tr>';

                orderedDatasets[season][type].forEach(function(dataset) {
                    dropDownHtml += '<tr>';
                    dropDownHtml += '<td class="jm-dataset-status">' + iam.main.createLabelDatasetStatus(dataset['status'], false) + '</td>';
                    dropDownHtml += '<td class="jm-dataset">';
                    dropDownHtml += '<a data-jm-dataset-id="' + dataset['dataset_id'] + '" href="#">';
                    dropDownHtml += dataset['dataset_id'];
                    dropDownHtml += '</a>';

                    if(typeof dataset['working_group'] !== 'undefined') {
                        dropDownHtml += ' ' + iam.main.createLabelWorkingGroup(dataset['working_group']);
                    }
                    
                    if(dataset['comment'] !== '') {
                        dropDownHtml += ' ' + iam.main.createLabelComment(dataset['comment']);
                    }

                    dropDownHtml += '</td>';
                    dropDownHtml += '</tr>';
                });
            }
            });
    });

    dropDownHtml += '</table></li>';

    menu.html(dropDownHtml);

    // Dataset selection view
    var datasetSelection = '<table class="table table-hover">';
    datasetSelection += '<thead>';
    datasetSelection += '<tr>';
    datasetSelection += '<th>';
    datasetSelection += 'Dataset Id';
    datasetSelection += '</th>';
    datasetSelection += '<th>';
    datasetSelection += '</th>';
    datasetSelection += '<th class="hidden-xs">';
    datasetSelection += 'Description';
    datasetSelection += '</th>';
    datasetSelection += '</tr>';
    datasetSelection += '</thead><tbody>';

    for(var i = 0; i < keys.length; ++i) {
        var dataset = datasets[keys[i]];

        var labels = '';

        if(typeof dataset['season'] !== 'undefined' && dataset['season'] !== null) {
            labels += ' ' + this.main.createLabelSeason(dataset['season']);
        }

        if(typeof dataset['type'] !== 'undefined' && dataset['type'] !== null) {
            labels += ' ' + this.main.createLabelDatasetType(dataset['type']);
        }

        if(typeof dataset['working_group'] !== 'undefined') {
            labels += ' ' + this.main.createLabelWorkingGroup(dataset['working_group']);
        }

        if(typeof dataset['comment'] !== 'undefined' && dataset['comment'] !== '') {
            labels += ' ' + this.main.createLabelComment(dataset['comment']);
        }

        if(dataset['selected']) {
            selected = dataset['dataset_id'];
        }

        datasetSelection += '<tr><td' + (dataset['supported'] ? '' : ' class="text-muted"') + '>';

        datasetSelection += dataset['dataset_id'];

        datasetSelection += '</td>';
        datasetSelection += '<td class="labels ' + (dataset['supported'] ? '' : ' text-muted') + '">';
        
        datasetSelection += labels;

        datasetSelection += '</td>';
        datasetSelection += '<td class="hidden-xs' + (dataset['supported'] ? '' : ' text-muted') + '">';
        datasetSelection += dataset['description'];
        datasetSelection += '</td></tr>';
    }

    datasetSelection += '</tbody></table>';

    $('a', menu).click(function(e) {
        $('a', menu).removeClass('bg-info');

        var datasetId = $(this).attr('data-jm-dataset-id');

        $(iam.currentDataset).html(datasetId).data('value', datasetId);
        $('a', this).addClass('bg-info');
        iam.updateCallback();
        e.preventDefault();

        // Modify URL
        iam.url.setState('dataset', datasetId);
        iam.url.pushState();
    });

    $('a', menu).each(function() {
        if($(this).attr('data-jm-dataset-id') == selected) {
            $(this).click();
            return;
        }
    });

    $(this.getContent()).html(datasetSelection);

    $('table tbody tr', this.getContent()).click(function() {
        var datasetId = $('td:first-child', this);

        if($(datasetId).hasClass('text-muted')) {
            // Not supported dataset
            return;
        }
    
        var datasetId = $(datasetId).html();

        $('a', menu).each(function() {
            if($(this).attr('data-jm-dataset-id') == datasetId) {
                $(this).click();
                return;
            }
        });
    });

    // Update title
    var title = '';
    var datasetId = this.currentDataset.data('value');

    if(typeof datasetId !== 'undefined') {
        title += '<h3><strong>';
        title += datasetId;
        title += '</strong>: ' + datasets[datasetId]['description'];
        title += '</h3>';
        title += '<p>';
        title += this.main.createLabelSeason(datasets[datasetId]['season'], true);

        if(typeof datasets[datasetId]['type'] !== 'undefined') {
            title += ' ' + this.main.createLabelDatasetType(datasets[datasetId]['type'], true);
        }

        if(typeof datasets[datasetId]['working_group'] !== 'undefined') {
            title += ' ' + this.main.createLabelWorkingGroup(datasets[datasetId]['working_group'], true);
        }

        if(typeof datasets[datasetId]['comment'] !== 'undefined' && datasets[datasetId]['comment'] !== '') {
            title += ' ' + this.main.createLabelComment(datasets[datasetId]['comment']);
        }

        title += ' ' + this.main.createLabelDatasetStatus(datasets[datasetId]['status'], true);

        title += '</p>';

        this.title.html(title);
        $('[data-toggle="tooltip"]', this.title).tooltip();
    }

    // Check if a dataset is selected
    // If not, check if a preselection exists via url
    if(typeof datasetId === 'undefined') {
        console.log('No dataset selcted yet');

        var preselection = this.url.getState('dataset');
        if(typeof preselection !== 'undefined') {
            console.log('Predefined dataset: ' + preselection);

            var validDataset = false;

            $('a', menu).each(function() {
                if($(this).attr('data-jm-dataset-id') == preselection && datasets[$(this).attr('data-jm-dataset-id')]['supported']) {
                    var click = this;

                    iam.nextActionCallback(function() {
                        $(click).click();
                    });

                    validDataset = true;
                    return;
                }
            });

            // Modify URL if dataset is not valid
            if(!validDataset) {
                console.log('Invalid dataset');

                iam.url.removeState('dataset');
                iam.url.pushState();
            }
        }
    }
}

JobMonitorDatasets.prototype.getSelectedDataset = function() {
    return $(this.currentDataset).data('value');
}

JobMonitorDatasets.prototype.startLoading = function() { 
    $('a[data-toggle="dropdown"]', this.datasetList).addClass('disabled');
    JobMonitorView.prototype.startLoading.call(this);
}

JobMonitorDatasets.prototype.endLoading = function() {
    $('a[data-toggle="dropdown"]', this.datasetList).removeClass('disabled');
    JobMonitorView.prototype.endLoading.call(this);
}

