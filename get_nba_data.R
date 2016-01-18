require(dplyr)
require(jsonlite)
require(magrittr)
require(stringr)
require(lubridate)
require(parallel)
require(tidyr)


# database connection -------------------------------------------------------------------------

host <- read.csv(".db_con", stringsAsFactors = FALSE)$host

nba_db <- src_postgres("nba", host)


# metadata df ---------------------------------------------------------------------------------

# set up df with (table_name, url) to abstract get_info/get_stats functions
metadata <- data_frame(table = c("teams",
                                 "players",
                                 "team_games",
                                 "player_shots",
                                 "play_by_play_scoring"),
                       id_type = c("team", NA, "team", "player", "game"),
                       url = c("http://stats.nba.com/stats/teaminfocommon?LeagueID=00&SeasonType=Regular+Season&TeamID=<id>&season=<season>",
                               "http://stats.nba.com/stats/commonallplayers?IsOnlyCurrentSeason=1&LeagueID=00&Season=<season>",
                               "http://stats.nba.com/stats/teamgamelog?LeagueID=00&Season=<season>&SeasonType=Regular+Season&TeamID=<id>",
                               paste0("http://stats.nba.com/stats/shotchartdetail?CFID=33&CFPARAMS=<season>",
                                      "&ContextFilter=&ContextMeasure=FGA&DateFrom=&DateTo=&GameID=",
                                      "&GameSegment=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base",
                                      "&Month=0&OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=PerGame",
                                      "&Period=0&PlayerID=<id>&PlusMinus=N&Position=&Rank=N",
                                      "&RookieYear=&Season=<season>&SeasonSegment=",
                                      "&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=",
                                      "&mode=Advanced&showDetails=0&showShots=1&showZones=1"),
                               paste0("http://stats.nba.com/stats/playbyplay?EndPeriod=10&EndRange=55800&GameID=",
                                      "<id>&RangeType=2",
                                      "&Season=<season>",
                                      "&SeasonType=Regular+Season&",
                                      "StartPeriod=1&StartRange=0")))

copy_to(nba_db, metadata, temporary = FALSE)

# function to get data from url

get_nba_data <- function(id = NA, table, season = "2015-16") {
    # make sure the table exists
    stopifnot(table %in% metadata$table)
    
    # players table doesn't need an id
    if (!is.na(id) && table == "players") {
        stop(paste0("Don't use the id parameter to get ", table, " data."))
    }
    
    # all other tables besides players need an id
    if (is.na(id) && table != "players") {
        stop(paste0("Set id = <", metadata$id_type[metadata$table == table], " id> to get data for ", table, "."))
    }
    
    # parameterize url
    url <- str_replace_all(metadata$url[metadata$table == table], "<season>", season)
    
    if (!is.na(id)) {
        url %<>% str_replace("<id>", id)
    }
    
    results <- fromJSON(url, flatten = TRUE)$resultSets
    
    headers <- results$headers[[1]] %>% tolower
    
    df <- results$rowSet[[1]] %>%
        as.data.frame(stringsAsFactors = FALSE)
    
    if (nrow(df) > 0) {
        colnames(df) <- headers
        df
    } else {
        NULL
    }
    
}


# players -------------------------------------------------------------------------------------

# udpate once daily

players <- get_nba_data(table = "players") %>%
    select(id = person_id, everything(), -contains("team"), team_id) %>%
    filter(games_played_flag == "Y", rosterstatus == "1")

copy_to(nba_db, players, temporary = FALSE)

# teams ---------------------------------------------------------------------------------------

# update once daily

teams <- mclapply(unique(players$team_id), get_nba_data, "teams", mc.cores = detectCores()) %>%
    bind_rows %>%
    rename(id = team_id)

copy_to(nba_db, teams, temporary = FALSE)


# player_shots --------------------------------------------------------------------------------

# update once daily

player_shots <- mclapply(players$id, get_nba_data, "player_shots", mc.cores = detectCores()) %>%
    bind_rows

copy_to(nba_db, player_shots, temporary = FALSE)

# team_games ----------------------------------------------------------------------------------

# update once daily

team_games <- mclapply(teams$id, get_nba_data, "team_games", mc.cores = detectCores()) %>%
    bind_rows %>%
    mutate_each("as.numeric", min:ncol(.)) %>%
    mutate(game_date = as.Date(mdy(game_date)),
           home = str_detect(matchup, "vs\\."))

copy_to(nba_db, team_games, temporary = FALSE)


# play_by_play --------------------------------------------------------------------------------


# initial state of each game
initial_state <- expand.grid(game_id = unique(team_games$game_id),
                             home = c(TRUE, FALSE),
                             stringsAsFactors = FALSE) %>%
    mutate(seconds_in = 0,
           score_margin = 0,
           period = 1,
           game_min = 12,
           game_sec = 0,
           score = "0 - 0")

# scoring plays
play_by_play_scoring <- mclapply(unique(team_games$game_id), get_nba_data, "play_by_play_scoring", mc.cores = detectCores()) %>%
    bind_rows %>%
    group_by(game_id) %>%
    filter(!is.na(scoremargin),
           period %in% 1:4,
           !duplicated(score)) %>%
    ungroup %>%
    inner_join(team_games) %>%
    mutate(score_margin = ifelse(scoremargin == "TIE", 0, scoremargin),
           score_margin = as.numeric(score_margin)*ifelse(home, 1, -1),
           period = as.numeric(period)) %>%
    select(game_id, home, period, game_clock = pctimestring, score, score_margin) %>%
    separate(game_clock, c("game_min", "game_sec"), convert = TRUE) %>%
    mutate(seconds_in = 720*period - (game_min*60 + game_sec)) %>%
    full_join(initial_state) %>%
    group_by(game_id, home) %>%
    arrange(seconds_in) %>%
    mutate(time_interval = cut(seconds_in, breaks = 97, labels = FALSE) - 1) %>%
    ungroup %>%
    group_by(game_id, home, time_interval) %>%
    slice(n()) %>%
    ungroup

copy_to(nba_db, play_by_play_scoring, temporary = FALSE)
