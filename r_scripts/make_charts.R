require(dplyr)
require(ggplot2)
require(viridis)
require(parallel)

db_con <- read.csv(".db_con", stringsAsFactors = FALSE)
nba_db <- src_postgres("nba", db_con$host)

# teams' typical score differential through regulation ----------------------------------------

scoring_summary <- tbl(nba_db, "play_by_play_scoring") %>%
    inner_join(tbl(nba_db, "team_games")) %>%
    inner_join(tbl(nba_db, "teams"), by = c("team_id" = "id")) %>%
    collect %>%
    group_by(team_name, time_interval, team_conference) %>%
    summarize(med_score_margin = median(score_margin)) %>%
    ungroup %>%
    group_by(team_name) %>%
    mutate(med_ending_diff = last(med_score_margin)) %>%
    ungroup %>%
    arrange(desc(med_ending_diff)) %>%
    mutate(team_name = factor(team_name, levels = unique(team_name))) %>%
    filter(time_interval %% 2 == 0)

ggplot(scoring_summary, aes(x = time_interval, y = med_score_margin)) +
    geom_step(lwd = 0.9,
              color = ifelse(scoring_summary$team_conference == "East",
                             "dodgerblue4", "firebrick4")) +
    facet_wrap(~team_name) +
    theme_bw()


# shooting_heatmaps ---------------------------------------------------------------------------

team_shots <- tbl(nba_db, "teams") %>%
    inner_join(tbl(nba_db, "player_shots"),
               by = c("id" = "team_id")) %>%
    select(team_id = id, team_abbreviation, loc_x, loc_y) %>%
    collect

pal <- plasma(32)

make_team_heatmap <- function(id) {
    team_df <- team_shots %>%
        filter(team_id == id)
    
    team_abbr <- tolower(team_df$team_ab[1])
    
    shot_hm <- ggplot(team_df, aes(x = loc_x, y = loc_y)) +
        geom_point(size = 0.8,
                   shape = 21,
                   fill = pal[1],
                   color = "white",
                   alpha = 0.08) +
        stat_density2d(aes(fill = ..level..), geom = "polygon") +
        scale_x_continuous(limits = c(-260, 260)) +
        scale_y_continuous(limits = c(-20, 290)) +
        scale_fill_gradientn(colors = pal, guide = FALSE) +
        theme_void() +
        theme(plot.margin = unit(rep(0, 4), "cm"),
              panel.margin = unit(rep(0, 4), "cm"))
    
    ggsave(paste0(team_abbr, "_shot_hm.svg"), shot_hm,
           width = 5.2, height = 3.3, units = "cm",
           path = "team_shot_heatmaps")
    
}

# save a shot heatmap for each team
mclapply(unique(team_shots$team_id), make_team_heatmap, mc.cores = detectCores())
