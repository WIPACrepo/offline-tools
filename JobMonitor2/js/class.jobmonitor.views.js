
function JobMonitorViews() {
    this.menu = $('#jm-view-dropdown');
}

JobMonitorViews.prototype.init = function(views) {
    $('.dropdown-menu li a', this.menu).click(function(e) {
        var target = $(this).attr('data-target');

        // true = checked, false = unchecked
        var prevState = $('.glyphicon', this).hasClass('glyphicon-check');

        if(prevState) {
            $('.glyphicon', this).removeClass('glyphicon-check');
            $('.glyphicon', this).addClass('glyphicon-unchecked');
        } else {
            $('.glyphicon', this).addClass('glyphicon-check');
            $('.glyphicon', this).removeClass('glyphicon-unchecked');
        }

        views[target].setVisible(!prevState);

        e.preventDefault();
    });
}

