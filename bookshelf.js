// database connection

var knex = require("knex")({
    client: "pg",
    connection: {
        host: "192.168.11.5",
        database: "nba",
        charsest: "utf8"
    }
});

module.exports = require("bookshelf")(knex);
