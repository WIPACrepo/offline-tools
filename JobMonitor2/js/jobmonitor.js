
$(document).ready(function() {
    $(document).on('click', '#jm-view-dropdown', function (e) {
        e.stopPropagation();
    });

    var jobMonitor = new JobMonitor();
});

