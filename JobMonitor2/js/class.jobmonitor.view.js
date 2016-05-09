
function JobMonitorView(name) {
    this.name = name;
}

JobMonitorView.prototype.init = function() {
    this.root = $('#jm-content-view-' + this.name);
    this.header = $('.jm-view-header', this.root);
    this.content = $('.jm-view-content', this.root);
}

JobMonitorView.prototype.getName = function() {return this.name;}
JobMonitorView.prototype.getHeader = function() {return this.header;}
JobMonitorView.prototype.getContent = function() {return this.content;}
JobMonitorView.prototype.updateView = function(data) {alert('Not implemented yet');}

