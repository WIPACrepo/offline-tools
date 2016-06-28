
function JobMonitorViews(url) {
    this.url = url;

    this.menu = $('#jm-view-dropdown');

    this.defaults = {'jobsView': false, 'calendarView': true};
}

JobMonitorViews.prototype.init = function(views) {
    var iam = this;

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

        iam.url.setState(target, !prevState);
        iam.url.pushState();

        e.preventDefault();
    });

    $('.dropdown-menu li a', this.menu).each(function() {
        var target = $(this).attr('data-target');

        var urlState = iam.url.getState(target);
        var buttonState = $('.glyphicon', this).hasClass('glyphicon-check');

        if(typeof urlState !== 'undefined') {
            var urlStateValue = urlState.toLowerCase() === 'true';

            if(!urlStateValue && urlState.toLowerCase() !== 'false') {
                iam.url.removeState(target);
                iam.url.pushState();

                // Use defaults
                if(buttonState != iam.defaults[target]) {
                    $(this).click();
                }
            } else if(urlStateValue != buttonState) {
                $(this).click();
            }
        } else {
            // Use defaults
            if(buttonState != iam.defaults[target]) {
                $(this).click();
            }
        }
    });
}

