var https = require('https');
var querystring = require('querystring');
var aws4  = require('aws4');
var is = require('is_js');

var ES_INDEX_EXISTS_STATUS_CODE = 200;
var ES_INDEX_MISSING_STATUS_CODE = 404;

var OPTION_TYPES = {
    index: 'string',
    type: 'string',
    path: 'string',
    accessKeyId: 'string',
    secretAccessKey: 'string',
    service: 'string',
    region: 'string',
    host: 'string',
    id: 'string',
    scroll: 'string',
    scrollId: 'string',
    body: 'json'
};

function validate_callback(callback) {
    if (!is.existy(callback)) throw new Error('not_callback');
    if (!is.function(callback)) throw new Error('invalid_callback');
}

function are_options_missing(options) {
    if (!is.existy(options)) { return 'not_options'; }
    if (!is.json(options)) { return 'invalid_options'; }

    for (var i = 1; i < arguments.length; i++) {
        var option = arguments[i];
        var option_value = options[option];

        if (!is.existy(option_value)) { return 'not_' + option; }

        var type = OPTION_TYPES[option];
        if (!is[type](option_value)) { return 'invalid_' + option; }
    }
}

function AWSES(config) {
    if (!is.existy(config)) { throw new Error('not_config'); }
    if (!is.json(config)) { throw new Error('invalid_config'); }

    var missing = are_options_missing(config, 'accessKeyId', 'secretAccessKey',
        'service', 'region', 'host');

    if (missing) { throw new Error(missing); }

    this.config = config;
};

AWSES.prototype._request = function(path, body, options, callback) {
    if (!is.existy(callback)) {
        if (is.function(options)) {
            callback = options;
            options = null;
        } else if (is.function(body)) {
            callback = body;
            body = null;
        } else if (is.function(path)) {
            callback = path;
            path = null;
        }
    }

    validate_callback(callback);

    if (!is.existy(body)) body = null;
    if (is.existy(body) && !is.json(body) && !is.array(body)) return callback('invalid_body');

    if (!is.existy(path)) return callback('not_path');
    if (!is.string(path)) return callback('invalid_path');

    var opts = {
        service: this.config.service,
        region: this.config.region,
        headers: {
            'Content-Type': 'application/json'
        },
        host: this.config.host,
        path: path
    }

    if (body && is.json(body)) {
        opts.body = JSON.stringify(body);
    } else if (body && is.array(body)) {
        opts.body = '';
        body.forEach(function(obj) {
            opts.body += JSON.stringify(obj);
            opts.body += '\n';
        });
    }

    if (options && options.method)
        opts.method = options.method;

    aws4.sign(
        opts,
        {
            accessKeyId: this.config.accessKeyId,
            secretAccessKey: this.config.secretAccessKey
        }
    );

    var fullResponse = '';

    var req = https.request(opts, function(res) {
        res.on('data', function (chunk) {
            fullResponse += chunk;
        });
        res.on('end', function() {
            try {
                return callback(null, JSON.parse(fullResponse));
            } catch (e) {
                return callback(e, res);
            }
        });
    });
    req.on('error', function(e) {
        return callback(e.message, null);
    });
    req.end(opts.body || '');
};

AWSES.prototype.createIndex = function(options, callback) {
    validate_callback(callback);

    var missing = are_options_missing(options, 'index');
    if (missing) { return callback(missing); }

    if (is.existy(options.body) && !is.json(options.body)) return callback('invalid_body');

    var path = '/' + options.index;
    this._request(path, options.body || {}, {method: 'PUT'}, callback);
};

AWSES.prototype.count = function(options, callback) {
    if (!is.existy(callback) && is.function(options)) {
        callback = options;
        options = null;
    }

    validate_callback(callback);

    var missing = are_options_missing(options, 'index', 'type');
    if (missing) { return callback(missing); }

    if (is.existy(options.body) && !is.json(options.body)) return callback('invalid_body');

    var path = '/' + options.index + '/' + options.type + '/_count';
    this._request(path, options.body, callback);
};

AWSES.prototype.index = function(options, callback) {
    if (!is.existy(callback) && is.function(options)) {
        callback = options;
        options = null;
    }

    validate_callback(callback);

    var missing = are_options_missing(options, 'index', 'type', 'body');
    if (missing) { return callback(missing); }

    if (is.existy(options.id) && !is.string(options.id)) return callback('invalid_id');

    var path = '/' + options.index + '/' + options.type;
    if (options.id)
        path += '/' + options.id;

    this._request(path, options.body, callback);
};

AWSES.prototype.update = function(options, callback) {
    if (!is.existy(callback) && is.function(options)) {
        callback = options;
        options = null;
    }

    validate_callback(callback);

    var missing = are_options_missing(options, 'index', 'type', 'body', 'id');
    if (missing) { return callback(missing); }

    var path = '/' + options.index + '/' + options.type + '/' + options.id + '/_update';
    this._request(path, options.body, callback);
};

AWSES.prototype.bulk = function(options, callback) {
    if (!is.existy(callback) && is.function(options)) {
        callback = options;
        options = null;
    }

    validate_callback(callback);
    var missing = are_options_missing(options, 'index', 'type');
    if (missing) { return callback(missing); }

    if (!is.existy(options.body)) return callback('not_body');
    if (!is.array(options.body) || (options.body.length == 0)) return callback('invalid_body');

    var path = '/' + options.index + '/' + options.type + '/_bulk';
    this._request(path, options.body, callback);
};

AWSES.prototype.search = function(options, callback) {
    if (!is.existy(callback) && is.function(options)) {
        callback = options;
        options = null;
    }

    validate_callback(callback);

    var missing = are_options_missing(options, 'index', 'type', 'body');
    if (missing) { return callback(missing); }

    if (is.existy(options.scroll) && !is.string(options.scroll)) return callback('invalid_scroll');
    if (is.existy(options.searchType) && !is.string(options.searchType)) return callback('invalid_searchType');
    if (is.existy(options.defaultOperator) && !is.string(options.defaultOperator)) return callback('invalid_defaultOperator');
    if (is.existy(options.size) && !is.integer(options.size)) return callback('invalid_size');
    if (is.existy(options.from) && !is.integer(options.from)) return callback('invalid_from');
    if (is.existy(options.sort) && !is.string(options.sort)) return callback('invalid_sort');

    var qs = {};
    if (options.scroll) qs.scroll = options.scroll;
    if (options.searchType) qs.search_type = options.searchType;
    if (options.size) qs.size = options.size;
    if (options.from) qs.from = options.from;
    if (options.sort) qs.sort = options.sort;

    if (is.existy(options.defaultOperator) && is.existy(options.body)
        && is.existy(options.body.query) && is.existy(options.body.query.query_string))
        options.body.query.query_string.default_operator = options.defaultOperator;

    var path = '/' + options.index + '/' + options.type + '/_search';
    path += '/?' + querystring.stringify(qs);

    this._request(path, options.body, callback);
};

AWSES.prototype.scroll = function(options, callback) {
    if (!is.existy(callback) && is.function(options)) {
        callback = options;
        options = null;
    }

    validate_callback(callback);

    var missing = are_options_missing(options, 'scroll', 'scrollId');
    if (missing) { return callback(missing); }

    var path = '/_search/scroll?scroll=' + options.scroll + '&scroll_id=' + options.scrollId;

    this._request(path, null, callback);
};

AWSES.prototype.get = function(options, callback) {
    if (!is.existy(callback) && is.function(options)) {
        callback = options;
        options = null;
    }

    validate_callback(callback);

    var missing = are_options_missing(options, 'index', 'type', 'id');
    if (missing) { return callback(missing); }

    var path = '/' + options.index + '/' + options.type + '/' + options.id;
    this._request(path, null, callback);
};

AWSES.prototype.mget = function(options, callback) {
    if (!is.existy(callback) && is.function(options)) {
        callback = options;
        options = null;
    }

    validate_callback(callback);

    var missing = are_options_missing(options, 'index', 'type', 'body');
    if (missing) { return callback(missing); }

    var path = '/' + options.index + '/' + options.type + '/_mget';
    this._request(path, options.body, callback);
};

AWSES.prototype.delete = function(options, callback) {
    if (!is.existy(callback) && is.function(options)) {
        callback = options;
        options = null;
    }

    validate_callback(callback);

    var missing = are_options_missing(options, 'index');
    if (missing) { return callback(missing); }

    if (is.existy(options.type) && !is.string(options.type)) return callback('invalid_type');

    if (is.existy(options.type) && is.existy(options.id) && !is.string(options.id)) return callback('invalid_id');

    var path = '/' + options.index;

    if (options.type)
        path += '/'+options.type;

    if (options.id)
        path += '/'+options.id;

    this._request(path, null, {method: 'DELETE'}, callback);
};

AWSES.prototype.putMapping = function(options, callback) {
    if (!is.existy(callback) && is.function(options)) {
        callback = options;
        options = null;
    }

    validate_callback(callback);

    var missing = are_options_missing(options, 'type', 'body');
    if (missing) { return callback(missing); }

    var path = '/';
    if (options.index)
        path += '/'+options.index;

    path += '/_mapping/'+options.type;
    this._request(path, options.body, {method: 'POST'}, callback);
};

AWSES.prototype.getMapping = function(options, callback) {
    if (!is.existy(callback) && is.function(options)) {
        callback = options;
        options = null;
    }

    validate_callback(callback);

    var missing = are_options_missing(options, 'type');
    if (missing) { return callback(missing); }

    var path = '/';
    if (options.index)
        path += '/'+options.index;

    path += '/_mapping/'+options.type;
    this._request(path, options.body, {method: 'GET'}, callback);
};

AWSES.prototype.indexExists = function(options, callback) {
    validate_callback(callback);

    if (!is.json(options)) return callback('not_options');
    if (!is.existy(options.index)) return callback('not_options.index');

    var path = '/' + options.index;
    this._request(path, null, {method: 'HEAD'}, function(err, data) {
        if (!data || !data.statusCode) return callback(err);
        var status = data.statusCode;
        var exists = status === ES_INDEX_EXISTS_STATUS_CODE;
        var not_exists = status === ES_INDEX_MISSING_STATUS_CODE;
        var failed = !(exists || not_exists);
        var error = failed ? new Error('status code ' + status) : null;

        callback(error, exists);
    });
};

module.exports = AWSES;
