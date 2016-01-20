var bookshelf = require("./bookshelf"),
    express = require("express"),
    app = express(),
    http = require("http"),
    server = http.createServer(app);

// models (set up relations here)
var Team = bookshelf.Model.extend({
        tableName: "teams"
    }),
    TeamGame = bookshelf.Model.extend({
        tableName: "team_games"
    }),
    PlayerShot = bookshelf.Model.extend({
        tableName: "player_shots"
    });

// static files
app.use(express.static("src"));

// views setup
app.set("views", __dirname + "/views");
app.set("view engine", "jade");

app.locals._ = require("lodash");

app.get("/", function(req, res) {
    res.render("index");
});

app.get("/teams", function(req, res) {
    new Team()
        .query(function(qb) {
            qb.orderBy("pct", "DESC")
        })
        .fetchAll() // change fetchAll() for all records
        .then(function(teams) {
            res.render("teams", { teams: teams.toJSON() });
        })
        .catch(function(err) {
            console.log(err);
            res.send("An error occurred...")
        });
});

app.get("/shots", function(req, res) {
    new PlayerShot()
        .query(function(qb) {
            qb.where("player_name", "=", "James Harden")
        })
        .fetchAll() // change fetchAll() for all records
        .then(function(player_shots) {
            res.render("player_page", { player_shots: player_shots.toJSON() });
        })
        .catch(function(err) {
            console.log(err);
            res.send("An error occurred...")
        });
});


// serve it up
var port = 4400;
server.listen(port, function() {
    console.log("Express server started on port " + port + "...");
});
