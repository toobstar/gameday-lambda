var https = require('https');
var moment = require('moment-timezone');
var util = require('util');
var zlib = require('zlib');

var config = require('./config.json');
var userAgent = util.format('xmlstats-exnode/%s (%s)', config.version, config.user_agent_contact);
var authorization = util.format('Bearer %s', config.access_token);

var shortDate = 'YYYYMMDD';
var longDate = 'dddd, MMMM D, YYYY';

function buildRequestConfig() {
    return {
        host: 'erikberg.com',
        // sport: undefined,
        sport: 'nba',
        // endpoint: 'events',
        endpoint: 'teams',
        id: undefined,
        format: 'json',
        params: {
        //     sport: 'nba',
            date: moment().format(shortDate)
         }
    };
}

// var method = 'nba/teams';
// var method = 'nba/results/'+teamId;
// var method = 'nba/boxscore/'+eventId;

function fetchTeams (callback) {
    var rc = buildRequestConfig();
    rc.endpoint = 'teams';
    console.log('rc',rc)
    fetcher(rc, callback);
}

function fetchGames (teamId, callback) {
    var rc = buildRequestConfig();
    rc.endpoint = 'results';
    rc.id = teamId;
    rc.params.date = moment().add(10, 'days').format(shortDate);
    console.log('rc',rc)
    fetcher(rc, callback);
}

function fetchResults (eventId, callback) {
    var rc = buildRequestConfig();
    rc.endpoint = 'boxscore';
    rc.id = eventId;
    console.log('rc',rc)
    fetcher(rc, callback);
}


function fetcher (requestConfig, callback) {
    httpGet(requestConfig, buildUrl(requestConfig), function (statusCode, contentType, data) {
        if (statusCode !== 200) {
            console.warn('Server did not return a "200 OK" response! ', statusCode, data);
            // XmlstatsError see https://erikberg.com/api/objects/xmlstats-error
            return;
        }
        callback(data);
    });
};

function httpGet(requestConfig, url, callback) {
    console.log('httpGet', url);

    var options = {
        hostname: requestConfig.host,
        path: url,
        headers: {
            'Accept-Encoding': 'gzip',
            Authorization: authorization,
            'User-Agent': userAgent
        }
    };

    var req = https.get(options, function(res) {
        var content;
        var data = [];

        if (res.headers['content-encoding'] === 'gzip') {
            var gzip = zlib.createGunzip();
            res.pipe(gzip);
            content = gzip;
        } else {
            content = res;
        }

        content.on('data', function (chunk) {
            data.push(chunk);
        });

        content.on('end', function() {
            var json = JSON.parse(Buffer.concat(data));
            callback(res.statusCode, res.headers['content-type'], json);
        });
    });

    req.on('error', function (err) {
        callback(500, 'text/plain', 'Unable to contact server: ' + err.message);
        console.error('Unable to contact server: %s', err.message);
    });
}

function formatDate(date, fmt) {
    return moment.tz(date, config.time_zone).format(fmt);
}

// See https://erikberg.com/api/endpoints#requrl Request URL Convention
function buildUrl(opts) {
    var ary = [opts.sport, opts.endpoint, opts.id];

    var path = ary.filter(function (element) {
        return element !== undefined;
    }).join('/');
    var url = util.format('https://%s/%s.%s', opts.host, path, opts.format);

    // check for parameters and create parameter string
    if (opts.params) {
        var paramList = [];
        for (var key in opts.params) {
            if (opts.params.hasOwnProperty(key)) {
                paramList.push(util.format('%s=%s',
                    encodeURIComponent(key), encodeURIComponent(opts.params[key])));
            }
        }
        var paramString = paramList.join('&');
        if (paramList.length > 0) {
            url += '?' + paramString;
        }
    }
    return url;
}


// For development/testing purposes
exports.handler = function(event, context, callback) {
  console.log('Running index.handler');
  console.log('==================================');
  console.log('event', event);
  console.log('==================================');
  console.log('Stopping index.handler');

  //  fetchTeams(function(result) {
  //    console.log('result', result);
  //});

    fetchGames('boston-celtics', function(result) {
        console.log('result', result);
    });

    //fetchResults('20171020-boston-celtics-at-philadelphia-76ers', function(result) {
    //    console.log('result', result);
    //});

  if (callback) {
    callback(null, event);
  }
};

exports.handler(); // just for local testing
