// database connection

var knex = require("knex")({
    client: "pg",
    connection: {
        host: "powerbear.local",
        database: "nba",
        charsest: "utf8"
    }
});

module.exports = require("bookshelf")(knex);
