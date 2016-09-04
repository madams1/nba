require(dplyr)
require(ggplot2)
require(viridis)
require(parallel)

db_con <- read.dcf(".db_con")
nba_db <- src_postgres("nba", db_con$host)

# shooting_heatmaps ---------------------------------------------------------------------------

team_shots <- tbl(nba_db, "teams") %>%
    inner_join(tbl(nba_db, "player_shots"),
               by = c("id" = "team_id")) %>%
    select(team_id = id, team_abbreviation, loc_x, loc_y) %>%
    collect(n = Inf)

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
           path = "src/team_shot_heatmaps", bg = "transparent")

}

# save a shot heatmap for each team
mclapply(unique(team_shots$team_id), make_team_heatmap, mc.cores = detectCores())
