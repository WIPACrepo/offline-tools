
function JobMonitorDatasets(updateCallback, url, nextActionCallback) {
    this.datasetList = $('#jm-dataset-dropdown');
    this.currentDataset = $('#current-dataset', this.datasetList);
    this.title = $('#jm-dataset-title');
    this.updateCallback = updateCallback;
    this.url = url;
    this.nextActionCallback = nextActionCallback;
}

JobMonitorDatasets.prototype = new JobMonitorView('dataset-selection');
JobMonitorDatasets.prototype.constructor = JobMonitorDatasets;

JobMonitorDatasets.prototype.update = function(datasets) {
    var iam = this; 
    var menu = $('.dropdown-menu', this.datasetList).empty();
    var selected = -1;

    // We want a sorted list
    var keys = Object.keys(datasets);
    keys.sort();
    keys.reverse();

    // Dataset selection view
    var datasetSelection = '<table class="table table-hover">';
    datasetSelection += '<thead>';
    datasetSelection += '<tr>';
    datasetSelection += '<th>';
    datasetSelection += 'Dataset Id';
    datasetSelection += '</th>';
    datasetSelection += '<th>';
    datasetSelection += 'Description';
    datasetSelection += '</th>';
    datasetSelection += '</tr>';
    datasetSelection += '</thead><tbody>';

    for(var i = 0; i < keys.length; ++i) {
        var dataset = datasets[keys[i]];

        var text = '<a href="#">';

        text += '<b>' + dataset['dataset_id'] + '</b>: ' + dataset['description'];

        text += '</a>';

        if(!dataset['supported']) {
            $(menu).append($('<li></li>').addClass('disabled').data('value', dataset['dataset_id']).html(text));
        } else {
            $(menu).append($('<li></li>').data('value', dataset['dataset_id']).html(text));
        }
        
        if(dataset['selected']) {
            selected = dataset['dataset_id'];
        }

        datasetSelection += '<tr><td' + (dataset['supported'] ? '' : ' class="text-muted"') + '>';

        datasetSelection += dataset['dataset_id'];

        datasetSelection += '</td>';
        datasetSelection += '<td' + (dataset['supported'] ? '' : ' class="text-muted"') + '>';
        datasetSelection += dataset['description'];
        datasetSelection += '</td></tr>';
    }

    datasetSelection += '</tbody></table>';

    $('li', menu).click(function(e) {
        $('li', menu).each(function() {
            $('a', this).removeClass('bg-info');
        });

        $(iam.currentDataset).html($(this).data('value')).data('value', $(this).data('value'));
        $('a', this).addClass('bg-info');
        iam.updateCallback();
        e.preventDefault();

        // Modify URL
        iam.url.setState('dataset', $(this).data('value'));
        iam.url.pushState();
    });

    $('li', menu).each(function() {
        if($(this).data('value') == selected) {
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

        $('li', menu).each(function() {
            if($(this).data('value') == datasetId) {
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
        title += '<span class="label label-info">Season ' + datasets[datasetId]['season'] + '</span>';

        if(typeof datasets[datasetId]['type'] !== 'undefined') {
            title += ' <span class="label label-info">Type ' + datasets[datasetId]['type'] + '</span>';
        }

        title += '</p>';

        this.title.html(title);
    }

    // Check if a dataset is selected
    // If not, check if a preselection exists via url
    if(typeof datasetId === 'undefined') {
        console.log('No dataset selcted yet');

        var preselection = this.url.getState('dataset');
        if(typeof preselection !== 'undefined') {
            console.log('Predefined dataset: ' + preselection);

            var validDataset = false;

            $('li', menu).each(function() {
                if($(this).data('value') == preselection && datasets[$(this).data('value')]['supported']) {
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

