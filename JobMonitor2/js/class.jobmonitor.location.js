
function JobMonitorLocation() {
    var iam = this;
    this.listener = [];

    window.addEventListener("popstate", function(e) {
        iam.forEach(function(listener) {
            listener.popState(iam);
        });
    });

    this.state = {};
}

JobMonitorLocation.prototype.init = function() {
    this.state = this.getQueryStrings();
}

JobMonitorLocation.prototype.addListener = function(listener) {
    this.listener.push(listener);
}

JobMonitorLocation.prototype.getState = function(name, defaultValue) {
    defaultvalue = typeof defaultValue === 'undefined' ? undefined : defaultValue;

    if(typeof this.state[name] === 'undefined') {
        return defaultValue;
    } else {
        return this.state[name];
    }
}

JobMonitorLocation.prototype.setState = function(name, value) {
    this.state[name] = value;
}

JobMonitorLocation.prototype.removeState = function(name) {
    if(typeof this.state[name] !== 'undefined') {
        delete this.state[name];
    }
}

JobMonitorLocation.prototype._createTitle = function() {
    var title = 'Offline Processing';
    
    if(typeof this.state['dataset'] !== 'undefined') {
        title += ': Dataset ' + this.state['dataset'];
    }

    if(typeof this.state['day'] !== 'undefined') {
        title += ' / Details for ' + this.state['day'];
    }

    return title;
}

JobMonitorLocation.prototype.pushState = function() {
    var url = '?' + jQuery.param(this.state);

    if(Object.keys(this.state).length === 0) {
        url = window.location.pathname;
    }

    var title = this._createTitle();

    document.title = title;

    history.pushState({'url': url}, title, url);
}

JobMonitorLocation.prototype.getQueryStrings = function() {
    // Source: http://stackoverflow.com/questions/2907482/how-to-get-the-query-string-by-javascript

    var assoc  = {};
    var decode = function (s) { return decodeURIComponent(s.replace(/\+/g, " ")); };
    var queryString = location.search.substring(1); 
    var keyValues = queryString.split('&'); 
    
    for(var i in keyValues) { 
        var key = keyValues[i].split('=');
        if (key.length > 1) {
            assoc[decode(key[0])] = decode(key[1]);
        }
    } 
    
    return assoc; 
} 

