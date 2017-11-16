require('dotenv').load();
var https = require('https');
var moment = require('moment-timezone');
var util = require('util');
var zlib = require('zlib');
var _ = require("underscore");
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
var processedDbName = 'b_processed';

var teamDb = cloudant.db.use(teamDbName);
var gamesDb = cloudant.db.use(gamesDbName);
var resultsDb = cloudant.db.use(resultsDbName);
var playersDb = cloudant.db.use(playersDbName);
var processedDb = cloudant.db.use(processedDbName);

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
    createDb(processedDbName);
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
            teamDb.insert(team, team.team_id).then(function(data) {
                console.log('created new teamDb', data);
            }).catch(function(err2) {
                console.log('teamDb something went wrong new', err2);
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
                gamesDb.insert(game, game.event_id).then(function(data) {
                    console.log('created new gamesDb', data);
                }).catch(function(err2) {
                    console.log('gamesDb something went wrong new', err2);
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
                calcScores(gameId);
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

function recalcScoresForFinished() {
    var gameFinishCutoff = moment().subtract(10, 'hours');
    resultsDb.list({include_docs:true}).then(function(body) {
        body.rows.forEach(function(doc, idx) {
            var gameId = doc.id;
            var gameStart = moment(doc.doc.event_information.start_date_time);
            // console.log(gameId, ' doc ', );
            console.log(gameId, ' game start ', gameStart.format());
            if (gameStart.isBefore(gameFinishCutoff)) {
                processedDb.get(gameId).then(function(gameProcessed) {
                    console.log('gameProcessed existing', gameProcessed);
                }).catch(function(err) {
                    console.log('no gameProcessed', gameId);
                    calcScores(gameId);
                });
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

function calcScores(gameId) {
    console.log('calcScores', gameId);
    var aussiePlayers = [];
    playersDb.list({include_docs:true}).then(function(body) {
        body.rows.forEach(function(doc, idx) {
            var birthplace = doc.doc.birthplace;
            if (birthplace && birthplace.toLowerCase().indexOf('australia') > -1) {
                console.log(doc.doc.display_name);
                aussiePlayers.push(doc.doc.display_name);
            }
        });

        resultsDb.get(gameId).then(function(gameResult) {
            processedDb.get(gameId).then(function(gameProcessed) {

            }).catch(function(err) {
                //console.log('calcScores-gameResult', gameResult);
                var gameProcessed = {};
                gameProcessed.id = gameId;
                gameProcessed.event_start_date_time = gameResult.event_information.start_date_time;
                gameProcessed.home_team = {};
                gameProcessed.home_team.id = gameResult.home_team.team_id;
                gameProcessed.home_team.abbreviation = gameResult.home_team.abbreviation;
                gameProcessed.home_team.full_name = gameResult.home_team.full_name;
                gameProcessed.away_team = {};
                gameProcessed.away_team.id = gameResult.away_team.team_id;
                gameProcessed.away_team.abbreviation = gameResult.away_team.abbreviation;
                gameProcessed.away_team.full_name = gameResult.away_team.full_name;
                if (gameResult && gameResult.home_period_scores && gameResult.away_period_scores) {

                    console.log("calcScores fullModel present ", gameId);

                    var hps = gameResult.home_period_scores;
                    var aps = gameResult.away_period_scores;

                    console.log("hps",hps);
                    console.log("aps",aps);

                    var homeWonQ1 = hps[0] > aps[0];
                    var homeWonQ2 = hps[1] > aps[1];
                    var homeWonQ3 = hps[2] > aps[2];
                    var homeWonQ4 = hps[3] > aps[3];

                    var leadChanges = 0;
                    if (homeWonQ1 != homeWonQ2) leadChanges++;
                    if (homeWonQ2 != homeWonQ3) leadChanges++;
                    if (homeWonQ3 != homeWonQ4) leadChanges++;

                    var q1Dif = Math.abs(hps[0]-aps[0]);
                    var q2Dif = Math.abs(hps[1]-aps[1]);
                    var q3Dif = Math.abs(hps[2]-aps[2]);
                    var q4Dif = Math.abs(hps[3]-aps[3]);

                    var score1Dif = Math.max((2-q1Dif),0);
                    var score2Dif = Math.max((4-q2Dif),0);
                    var score3Dif = Math.max((6-q3Dif),0);
                    var score4Dif = Math.max((8-q4Dif),0);  // less difference = greater score (with weighting towards final quarter)
                    var scoreDif = score1Dif + score2Dif + score3Dif + score4Dif;

                    if (hps.length > 4) { // overtime
                        var otDif = Math.abs(hps[4]-aps[4]);
                        var scoreOtDif = Math.max((5-otDif),3);
                        scoreDif += scoreOtDif;

                        var homeWonOt1 = hps[4] > aps[4];
                        if (homeWonOt1 != homeWonQ4) leadChanges++;

                        if (hps.length > 5) { // OT2
                            otDif = Math.abs(hps[5]-aps[5]);
                            scoreOtDif = Math.max((5-otDif),3);
                            scoreDif += scoreOtDif;

                            var homeWonOt2 = hps[4] > aps[4];
                            if (homeWonOt2 != homeWonOt1) leadChanges++;
                        }
                    }

                    scoreDif = scoreDif + (leadChanges*3);
                    console.log("leadChanges " + leadChanges + " adding " + (leadChanges*3) + " scoreDif now: " + scoreDif);

                    //        console.log("hps",homeWonQ1,homeWonQ2,homeWonQ3,homeWonQ4);
                    //        console.log("scoreDif",scoreDif);
                    //        console.log("diffs",q1Dif,q2Dif,q3Dif,q4Dif);

                    var totalDifference = (q1Dif+q2Dif+q3Dif+q4Dif);
                    var finalDifference = q4Dif;

                    console.log("leadChanges",leadChanges);
                    console.log("totalDifference",totalDifference);
                    console.log("finalDifference",finalDifference);

                    gameProcessed.pointsTotalDiff = totalDifference;
                    gameProcessed.pointsFinalDiff = finalDifference;
                    gameProcessed.leadChanges = leadChanges;
                    gameProcessed.pointsBasedScore = scoreDif;

                    if (scoreDif > 15)
                        gameProcessed.pointsBasedRating = 'A';
                    else if (scoreDif > 10)
                        gameProcessed.pointsBasedRating = 'B';
                    else
                        gameProcessed.pointsBasedRating = 'C';
                }

                if (gameResult && gameResult.away_stats && gameResult.home_stats) {
                    gameProcessed.aussies = [];
                    //var ausPlayers = ['EXUM','BAIRSTOW','BOGUT','PATTY','MILLS','INGLES','DELLAVEDOVA','MOTUM','BAYNES','MAKER'];
                    _.each(gameResult.away_stats.concat(gameResult.home_stats),function(stat){
                        //console.log("checking player: ", stat.display_name, aussiePlayers);
                        if (_.contains(aussiePlayers, stat.display_name)) {
                            console.log("--found aussie: ", stat.display_name);
                            gameProcessed.aussies.push(
                                {
                                    'name':stat.display_name,
                                    'minutes':stat.minutes,
                                    'points':stat.points,
                                    'assists':stat.assists,
                                    'turnovers':stat.turnovers,
                                    'steals':stat.steals,
                                    'blocks':stat.blocks,
                                    'field_goal_percentage':stat.field_goal_percentage,
                                    'three_point_percentage':stat.three_point_percentage,
                                    'free_throw_percentage':stat.free_throw_percentage
                                }
                            );
                        }
                    });
                }

                if (gameResult && gameResult.home_totals && gameResult.home_totals.points) {
                    if (Math.random() * 10 > 5) { // randomise order of score display
                        console.log("a) setting finalScore for ", gameId, gameProcessed.finalScore);
                        gameProcessed.finalScore = gameResult.away_totals.points + '/' + gameResult.home_totals.points;
                    }
                    else {
                        console.log("b) setting finalScore for ", gameId, gameProcessed.finalScore);
                        gameProcessed.finalScore = gameResult.home_totals.points + '/' + gameResult.away_totals.points;
                    }
                }

                console.log("gameProcessed",gameProcessed);

                processedDb.insert(gameProcessed, gameId).then(function(data) {
                    console.log('created new gameProcessed', data);
                }).catch(function(err2) {
                    console.log('gameProcessed something went wrong new', err2);
                });
            });
        }).catch(function(err) {
            console.error('result', err);
        });


    }).catch(function(err) {
        console.log('something went wrong', err);
    });


}

// For development/testing purposes
exports.handler = function(event, context, callback) {
  // console.log('Running index.handler');
  // console.log('==================================');
  // console.log('event', event);
  // console.log('==================================');
  // console.log('Stopping index.handler');

    // createDbs()
    //setupGames();
    fetchResultsForGames();
    //setupPlayers();
    // recalcScoresForFinished();

  if (callback) {
    callback(null, event);
  }
};

// exports.handler(); // just for local testing
