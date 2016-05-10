
function JobMonitorDatasets(updateCallback) {
    this.datasetList = $('#jm-dataset-dropdown');
    this.currentDataset = $('#current-dataset', this.datasetList);
    this.updateCallback = updateCallback;
}

JobMonitorDatasets.prototype.update = function(datasets) {
    var iam = this; 
    var menu = $('.dropdown-menu', this.datasetList).empty();
    var selected = -1;

    // We want a sorted list
    var keys = Object.keys(datasets);
    keys.sort();
    keys.reverse();

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
    }

    $('li', menu).click(function() {
        $('li', menu).each(function() {
            $('a', this).removeClass('bg-info');
        });

        $(iam.currentDataset).html($(this).data('value')).data('value', $(this).data('value'));
        $('a', this).addClass('bg-info');
        iam.updateCallback();
    });

    $('li', menu).each(function() {
        if($(this).data('value') == selected) {
            $(this).click();
            return;
        }
    });
}

JobMonitorDatasets.prototype.getSelectedDataset = function() {
    return $(this.currentDataset).data('value');
}

JobMonitorDatasets.prototype.startLoading = function() { 
    $('a[data-toggle="dropdown"]', this.datasetList).addClass('disabled');
}

JobMonitorDatasets.prototype.endLoading = function() {
    $('a[data-toggle="dropdown"]', this.datasetList).removeClass('disabled');
}

