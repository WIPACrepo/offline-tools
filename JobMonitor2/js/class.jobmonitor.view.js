
function JobMonitorView(name) {
    this.name = name;
    this.visible = true;
}

JobMonitorView.prototype.init = function() {
    this.root = $('#jm-content-view-' + this.name);
    this.header = $('.jm-view-header', this.root);
    this.content = $('.jm-view-content', this.root);
}

JobMonitorView.prototype.getName = function() {return this.name;}
JobMonitorView.prototype.getHeader = function() {return this.header;}
JobMonitorView.prototype.getContent = function() {return this.content;}
JobMonitorView.prototype.updateView = function(data) {console.log('update() Not implemented yet');}

JobMonitorView.prototype.show = function(force) {
    force = typeof force === 'undefined' ? false : force;

    if(this.visible) {
        $(this.root).show('slow');
    }
}

JobMonitorView.prototype.hide = function() {
    $(this.root).hide('slow');
}

JobMonitorView.prototype.startLoading = function() {
    $(this.getContent()).animate({'opacity': .5});
}

JobMonitorView.prototype.endLoading = function() {
    $(this.getContent()).animate({'opacity': 1});
}

JobMonitorView.prototype.setVisible = function(visible) {
    this.visible = visible;

    if(visible) {
        this.show();
    } else {
        this.hide();
    }
}

