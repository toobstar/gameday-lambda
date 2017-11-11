require('dotenv').load();
var https = require('https');
var moment = require('moment-timezone');
var util = require('util');
var zlib = require('zlib');
var FunctionQueue = require('functionqueue');
var Cloudant = require('cloudant');

var config = require('./config.json');
var userAgent = util.format('xmlstats-exnode/%s (%s)', config.version, config.user_agent_contact);
var authorization = util.format('Bearer %s', config.access_token);

var shortDate = 'YYYYMMDD';
var longDate = 'dddd, MMMM D, YYYY';

var username =    process.env.cloudant_username;
var password =    process.env.cloudant_password;

console.log('cloudant', username, password);
var cloudant = Cloudant({account:username, password:password, plugin:'promises'});

var teamDbName =    'b_teams';
var gamesDbName =   'b_games';
var resultsDbName = 'b_results';
var playersDbName = 'b_players';

var teamDb = cloudant.db.use(teamDbName);
var gamesDb = cloudant.db.use(gamesDbName);
var resultsDb = cloudant.db.use(resultsDbName);
var playersDb = cloudant.db.use(playersDbName);

var nbaQ = new FunctionQueue(5, 60, 5, "NBA-API"); // limit is 6 per minute.  5 gives a small buffer

function createDb(dbname) {
    cloudant.db.create(dbname).then(function (data) {
        console.log("create result", dbname, data);
        db = cloudant.db.use(dbname);
        var security = {
            nobody: ['_reader', '_replicator'],
            apiKey: ['_reader', '_replicator'],
            bestgametowatch: ['_reader', '_writer', '_admin', '_replicator']
        };
        db.set_security(security, function (er, result) {
            if (er) {
                throw er;
            }
            console.log("set_security result", result);
        });
    }).catch(function (err) {
        console.log('create db error ', dbname, err);
    });
}

function buildRequestConfig(endpoint, id) {
    return {
        host: 'erikberg.com',
        // sport: undefined,
        sport: 'nba',
        // endpoint: 'events',
        endpoint: endpoint,
        id: id,
        format: 'json'
        // ,
        // params: {
        //     date: moment().format(shortDate)
        //  }
    };
}

function fetchTeams (callback) {
    fetcher(buildRequestConfig('teams'), callback);
}

function fetchGames (teamId, callback) {
    fetcher(buildRequestConfig('results', teamId), callback);
}

function fetchPlayers (teamId, callback) {
    fetcher(buildRequestConfig('roster', teamId), callback);
}

function fetchResults (eventId, callback) {
    fetcher(buildRequestConfig('boxscore', eventId), callback);
}

function fetcher (requestConfig, callback) {
    console.log('requestConfig',requestConfig)
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

function createDbs() {
    createDb(teamDbName);
    createDb(gamesDbName);
    createDb(resultsDbName);
    createDb(playersDbName);
}


function removeDbs() {
    cloudant.db.list(function(err, body) {
        body.forEach(function(db) {
            console.log(db);
            cloudant.db.destroy(db).then(function (data) {
               console.log("destroy result",  data);
            }).catch(function (err) {
               console.log('destroy db error ',  err);
            });
        });
    });
}

function setupTeams() {
    fetchTeams(function(teams) {
        teams.forEach(function (team) {
            console.log('team', team);
            teamDb.insert(team, team.team_id, function(err, body) {
                if (err) {
                    console.error(err);
                }
                else {
                    console.log(body);
                }
            });
        });

    });
}

var fetchGamesFn = function (teamId) {
    console.log('fetchGamesFn', teamId);
    fetchGames(teamId, function(games) {
        console.log('size', teamId, games.length);
        games.forEach(function (game) {
            gamesDb.get(game.event_id).then(function(existingResult) {
                console.log('game already there', game.event_id);
            }).catch(function(err2) {
                gamesDb.insert(game, game.event_id, function(err, body) {
                    if (err) {
                        console.error(err);
                    }
                    else {
                        console.log('game added', game.event_id);
                    }
                });
            });
        });
    });
}

function setupGames() {
    teamDb.list(function(err, body) {
        if (!err) {
            body.rows.forEach(function(doc) {
                var teamId = doc.id;
                nbaQ.scheduleFn(fetchGamesFn, [teamId])
            });
        }
    });
}

function isNull(results) {
    return results == null ? 0 : (results[1] || 0);
}

var fetchResultsFn = function (gameId) {
    console.log('fetchResultsFn', gameId);

    resultsDb.get(gameId).then(function(existingResult) {
        console.log('existingResult', !isNull(existingResult));
    }).catch(function(err2) {

        fetchResults(gameId, function(result) {
            console.log('result', gameId, result.length);

            resultsDb.insert(result, gameId).then(function(data) {
                console.log('created new result', data);
            }).catch(function(err2) {
                console.log('something went wrong new', err2);
            });
        });

    });
}

function fetchResultsForGames() {
    var gameFinishCutoff = moment().subtract(10, 'hours');
    gamesDb.list({include_docs:true}).then(function(body) {
        //console.log(body);
        body.rows.forEach(function(doc, idx) {
            var gameId = doc.id;
            var gameStart = moment(doc.doc.event_start_date_time);
            //console.log('game start ', gameStart.format());
            if (gameStart.isBefore(gameFinishCutoff)) {
                console.log('game data available ', gameStart.format());
                nbaQ.scheduleFn(fetchResultsFn, [gameId])
            }
        });
    }).catch(function(err) {
        console.log('something went wrong', err);
    });
}

var fetchPlayersFn = function (teamId) {
    console.log('fetchPlayersFn', teamId);
    fetchPlayers(teamId, function(playerResult) {
        var players = playerResult.players;
        console.log('size', teamId, players.length);
        players.forEach(function (player) {
            player.teamId = teamId;
            playersDb.get(player.display_name, { revs_info: true }).then(function(existingResult) {
                console.log('existingResult', !isNull(existingResult));
                if (existingResult) {
                    player._id = existingResult._id;
                    player._rev = existingResult._rev;
                    playersDb.insert(player, player.display_name).then(function(data) {
                        console.log('updated existing player', data);
                    }).catch(function(err) {
                        console.log('something went wrong updated', err);
                    });
                }
            }).catch(function(err) {
                playersDb.insert(player, player.display_name).then(function(data) {
                    console.log('created new player', data);
                }).catch(function(err2) {
                    console.log('something went wrong new', err2);
                });
            });
        });
    });
}

function setupPlayers() {
    teamDb.list(function(err, body) {
        if (!err) {
            console.log('body', body);
            body.rows.forEach(function(doc, idx) {
                var teamId = doc.id;
                nbaQ.scheduleFn(fetchPlayersFn, [teamId])
            });
        }
    });
}

// For development/testing purposes
exports.handler = function(event, context, callback) {
  // console.log('Running index.handler');
  // console.log('==================================');
  // console.log('event', event);
  // console.log('==================================');
  // console.log('Stopping index.handler');

    //setupGames();
    fetchResultsForGames();
    //setupPlayers();

    // fetchPlayers('atlanta-hawks', function(result) {
    //     console.log('result', result);
    // });

     // teams.forEach(function (team) {
     //     console.log('team', team);
     //     teamDb.insert(team, team.team_id, function(err, body) {
     //         if (err) {
     //             console.error(err);
     //         }
     //         else {
     //             console.log(body);
     //         }
     //     });
     // });

    // fetchResults('20171020-boston-celtics-at-philadelphia-76ers', function(result) {
    //    // console.log('result', result);
    //     console.log('size', result.length);
    // });

  if (callback) {
    callback(null, event);
  }
};

exports.handler(); // just for local testing
