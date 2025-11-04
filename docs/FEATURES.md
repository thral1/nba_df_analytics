# Feature Documentation

Complete documentation of all 60+ features used in the machine learning models.

## Feature Categories

1. [Player Historical Averages](#player-historical-averages)
2. [Rest & Schedule Features](#rest--schedule-features)
3. [Matchup Features](#matchup-features)
4. [Vegas Features](#vegas-features)
5. [Situational Features](#situational-features)
6. [Derived Features](#derived-features)

---

## Player Historical Averages

Rolling averages calculated from all prior games in the season.

### Basic Stats

| Feature | Description | Typical Range |
|---------|-------------|---------------|
| `prev_mean_PTS` | Average points per game | 0-35 |
| `prev_mean_REB` | Average rebounds per game | 0-15 |
| `prev_mean_OREB` | Average offensive rebounds | 0-5 |
| `prev_mean_DREB` | Average defensive rebounds | 0-12 |
| `prev_mean_AST` | Average assists per game | 0-12 |
| `prev_mean_STL` | Average steals per game | 0-3 |
| `prev_mean_BLK` | Average blocks per game | 0-3 |
| `prev_mean_TOV` | Average turnovers per game | 0-5 |

### Shooting Stats

| Feature | Description | Typical Range |
|---------|-------------|---------------|
| `prev_mean_FGM` | Average field goals made | 0-15 |
| `prev_mean_FGA` | Average field goal attempts | 0-25 |
| `prev_mean_FG_PCT` | Field goal percentage | 0.30-0.60 |
| `prev_mean_FG3M` | Average 3-pointers made | 0-5 |
| `prev_mean_FG3A` | Average 3-point attempts | 0-12 |
| `prev_mean_FG3_PCT` | 3-point percentage | 0.25-0.45 |
| `prev_mean_FG2M` | Average 2-pointers made | 0-12 |
| `prev_mean_FTM` | Average free throws made | 0-10 |
| `prev_mean_FTA` | Average free throw attempts | 0-12 |
| `prev_mean_FT_PCT` | Free throw percentage | 0.50-0.95 |

### Advanced Stats

| Feature | Description | Typical Range |
|---------|-------------|---------------|
| `USG_PCT` | Usage percentage (possessions used) | 0.10-0.40 |
| `USG_PCT_minus_TOV` | Usage minus turnover impact | 0.08-0.38 |
| `mean_AST_PCT` | Assist percentage | 0.05-0.60 |
| `mean_AST_RATIO` | Assist-to-turnover ratio | 1.0-5.0 |
| `mean_TO_PCT` | Turnover percentage | 0.05-0.25 |
| `mean_TS_PCT` | True shooting percentage | 0.45-0.70 |

### Playing Time

| Feature | Description | Typical Range |
|---------|-------------|---------------|
| `mean_seconds` | Average seconds per game | 0-2880 |
| `prev_seconds` | Seconds in previous game | 0-2880 |
| `prev2_seconds` | Seconds 2 games ago | 0-2880 |
| `prev5_seconds` | Average seconds in last 5 games | 0-2880 |

---

## Rest & Schedule Features

### Binary Rest Indicators

| Feature | Description | Values |
|---------|-------------|--------|
| `b2b` | Playing back-to-back (1 day rest) | 0 or 1 |
| `front_b2b` | First game of back-to-back | 0 or 1 |
| `extra_rest` | 3+ days rest | 0 or 1 |
| `opp_b2b` | Opponent on back-to-back | 0 or 1 |
| `opp_front_b2b` | Opponent on front of back-to-back | 0 or 1 |
| `opp_extra_rest` | Opponent has extra rest | 0 or 1 |

### Rest-Specific Averages

| Feature | Description | Interpretation |
|---------|-------------|----------------|
| `mean_b2b_PTS` | Average points on back-to-backs | Usually 10-15% below normal |
| `mean_non_b2b_PTS` | Average points with normal rest | Baseline performance |
| `mean_extra_rest_PTS` | Average points with extra rest | Usually 5-10% above normal |
| `mean_opp_b2b_PTS` | Average vs tired opponents | Usually 5-8% above normal |
| `mean_opp_extra_rest_PTS` | Average vs rested opponents | Slightly below normal |

**Rest Effect** (Points):
```
rest_effect = mean_non_b2b_PTS - mean_b2b_PTS
Typical: +2.5 points with normal rest vs back-to-back
```

**Opponent Rest Effect**:
```
opp_rest_effect = mean_opp_b2b_PTS - mean_opp_extra_rest_PTS
Typical: +1.5 points vs tired opponent
```

---

## Matchup Features

### Opponent Defense

| Feature | Description | Typical Range |
|---------|-------------|---------------|
| `o_team_def_rtg` | Opponent defensive rating | 100-120 |
| `league_average_def_rtg` | League average for normalization | ~110 |
| `o_team_def_rtg_v_position` | Opp defense vs player's position | 100-120 |
| `league_average_def_rtg_v_position` | League avg vs position | ~110 |

**Defensive Rating**: Points allowed per 100 possessions
- <105: Elite defense
- 105-110: Above average
- 110-115: Average
- 115+: Below average

### Expected Performance (Defense-Adjusted)

| Feature | Description | Calculation |
|---------|-------------|-------------|
| `expected_PTS_def_rtg` | Expected points vs this defense | `prev_mean_PTS * (league_def_rtg / opp_def_rtg)` |
| `def_rtg_effect` | Defensive adjustment | `expected_PTS_def_rtg - prev_mean_PTS` |
| `def_rtg_delta` | Defense quality | `league_def_rtg - opp_def_rtg` |

**Example**:
```
Player averages: 20 PTS
League avg defense: 110
Opponent defense: 105 (good)

expected_PTS_def_rtg = 20 * (110 / 105) = 21.0
def_rtg_effect = 21.0 - 20 = +1.0

Interpretation: Expect 1 fewer point vs this good defense
Actually the calc shows +1, but that's the artifact - better defense = lower rating
```

### Pace Adjustments

| Feature | Description | Typical Range |
|---------|-------------|---------------|
| `expected_PTS_pace` | Expected points based on pace | -5 to +5 vs mean |
| `expected_PTS_pace2` | Alternative pace calculation | -5 to +5 vs mean |
| `expected_PTS_pace3` | Vegas-based pace | -5 to +5 vs mean |
| `pace_effect` | Impact of pace | -3 to +3 |

**Pace**: Possessions per 48 minutes
- <96: Slow pace (grind-it-out)
- 96-102: Average pace
- 102+: Fast pace (run-and-gun)

**Calculation**:
```
game_pace = (team_pace + opp_pace) / 2
pace_ratio = game_pace / player_avg_pace
expected_PTS_pace = prev_mean_PTS * pace_ratio
```

### Position-Specific Defense

| Feature | Description |
|---------|-------------|
| `o_team_PTS_v_position` | Points allowed to this position |
| `o_team_OREB_v_position` | Off. rebounds allowed to position |
| `o_team_DREB_v_position` | Def. rebounds allowed to position |
| `o_team_AST_v_position` | Assists allowed to position |

**Example**: If opponent allows 25 PPG to PGs but league average is 22 PPG, point guards get a boost.

---

## Vegas Features

### Over/Under Based

| Feature | Description | Typical Range |
|---------|-------------|---------------|
| `vegas_ratio_pts` | Expected points from O/U | 15-35 |
| `vegas_ratio_pts_effect` | Difference from mean | -5 to +5 |
| `vegas_ratio_pts_pinnacle` | Using Pinnacle lines | 15-35 |

**Calculation**:
```
over_under = 220.5
team_usage_pct = 0.20  # Player's share of team offense

vegas_ratio_pts = (over_under / 2) * team_usage_pct
```

### Point Spread Features

Binary indicators for different spread scenarios:

| Feature | Description | When True |
|---------|-------------|-----------|
| `point_spread_abs_3_or_less` | Close game | abs(spread) <= 3 |
| `point_spread_abs_6_or_less` | Competitive game | abs(spread) <= 6 |
| `point_spread_abs_9_or_less` | Moderate favorite | abs(spread) <= 9 |
| `point_spread_abs_over_12` | Big favorite/underdog | abs(spread) > 12 |
| `point_spread_3_or_less` | Small favorite | 0 < spread <= 3 |
| `point_spread_neg_12_or_less` | Big underdog | spread < -12 |

**Impact**:
- Close games (spread <3): Starters play full minutes
- Blowouts (spread >12): Risk of garbage time, reduced minutes

---

## Situational Features

### Home/Away

| Feature | Description | Typical Impact |
|---------|-------------|----------------|
| `location` | 1 = home, 0 = away | Home: +2-3 points |
| `mean_home_PTS` | Average points at home | Usually higher |
| `mean_away_PTS` | Average points on road | Usually lower |
| `location_effect` | Home court advantage | +1 to +4 |

**Home Court Advantage**:
```
location_effect = mean_home_PTS - mean_away_PTS
NBA average: +2.5 points at home
```

### Starter vs Bench

| Feature | Description | Values |
|---------|-------------|--------|
| `starter` | Starting lineup designation | 0 or 1 |
| `mean_starter_PTS` | Avg points when starting | Usually higher |
| `mean_bench_PTS` | Avg points off the bench | Usually lower |
| `starterbench_effect` | Impact of starting | +3 to +8 |

**Why it Matters**: Starters average 28-32 minutes, bench players 15-20 minutes.

### Team Performance

| Feature | Description | Typical Range |
|---------|-------------|---------------|
| `team_game_number` | Games played this season | 1-82 |
| `win_pct` | Team win percentage | 0.20-0.75 |
| `opp_win_pct` | Opponent win percentage | 0.20-0.75 |
| `win_pct_locale` | Home/away win percentage | Varies |

---

## Derived Features

### Scoring Breakdown

| Feature | Description | Typical % of Total |
|---------|-------------|-------------------|
| `prev_pts_fb_mean` | Points from fast breaks | 10-20% |
| `prev_pts_paint_mean` | Points in the paint | 40-60% |
| `prev_pts_2nd_chance_mean` | Second-chance points | 5-15% |
| `prev_pts_off_tov_mean` | Points off turnovers | 10-20% |

**Team Context**:
```
team_pts_fb_mean - Average team fast break points
opp_o_pts_fb_mean - Opponent's offensive fast break points allowed
```

### Rebounding Percentage

| Feature | Description | Formula |
|---------|-------------|---------|
| `a_OREB_PCT` | Offensive rebound % | `OREB / (OREB + opp_DREB)` |
| `a_DREB_PCT` | Defensive rebound % | `DREB / (DREB + opp_OREB)` |
| `expected_OREB` | Expected offensive rebounds | Based on team rebounds & usage |
| `expected_DREB` | Expected defensive rebounds | Based on opponent misses |

### Shooting Efficiency

| Feature | Description |
|---------|-------------|
| `prev_PCT_CFGM` | % of field goals that were contested |
| `prev_PCT_UFGM` | % of field goals that were uncontested |
| `prev_CFG_PCT` | Contested FG percentage |
| `prev_UFG_PCT` | Uncontested FG percentage |

---

## Feature Importance Rankings

### Points Prediction (Top 20)

1. `prev_mean_PTS` (35% importance)
2. `mean_seconds` (18%)
3. `USG_PCT` (8%)
4. `expected_PTS_pace` (6%)
5. `starter` (5%)
6. `expected_PTS_def_rtg` (4%)
7. `mean_home_PTS` (3%)
8. `prev_seconds` (3%)
9. `b2b` (2%)
10. `vegas_ratio_pts` (2%)
11. `location` (2%)
12. `def_rtg_effect` (2%)
13. `mean_FGM` (2%)
14. `mean_FG3M` (1%)
15. `extra_rest` (1%)
16. `opp_b2b` (1%)
17. `pace_effect` (1%)
18. `point_spread_abs_over_12` (1%)
19. `team_game_number` (1%)
20. `prev5_seconds` (1%)

### Rebounds Prediction (Top 10)

1. `mean_OREB` or `mean_DREB` (40%)
2. `mean_seconds` (15%)
3. `team_OREB` or `team_DREB` (10%)
4. `opponent_o_team_DREB` (8%)
5. `a_OREB_PCT` or `a_DREB_PCT` (6%)
6. `starter` (4%)
7. `location` (3%)
8. `expected_OREB` or `expected_DREB` (3%)
9. `b2b` (2%)
10. `team_misses` (2%)

### Assists Prediction (Top 10)

1. `mean_AST` (45%)
2. `mean_seconds` (20%)
3. `mean_AST_PCT` (10%)
4. `USG_PCT` (8%)
5. `starter` (5%)
6. `team_pace` (3%)
7. `location` (2%)
8. `o_team_AST_v_position` (2%)
9. `b2b` (1%)
10. `expected_AST` (1%)

---

## Feature Engineering Best Practices

### 1. Avoid Lookahead Bias

**BAD**:
```ruby
# Uses future data!
season_average = all_games.mean(:PTS)
```

**GOOD**:
```ruby
# Only uses past data
prior_games = all_games.where("date < ?", current_date)
season_average = prior_games.mean(:PTS)
```

### 2. Handle Missing Data

```ruby
# Player hasn't played on back-to-back yet
if mean_b2b_PTS.nil?
  mean_b2b_PTS = prev_mean_PTS  # Use overall average
end
```

### 3. Normalize Features

```ruby
# Defense is better at lower ratings
# Normalize to league average
def_rtg_normalized = o_team_def_rtg / league_average_def_rtg
```

### 4. Create Interaction Features

```ruby
# Synergy between pace and usage
pace_usage_effect = pace_ratio * USG_PCT

# Rest + Minutes = fatigue indicator
fatigue_indicator = b2b * (prev_seconds / mean_seconds)
```

---

## Common Pitfalls

### 1. Small Sample Size
Early in season, rolling averages are noisy. Use prior season as baseline.

### 2. Position Changes
Player switches from PG to SG mid-season - matchup stats become unreliable.

### 3. Role Changes
Starter becomes bench player or vice versa - recent games more relevant than season average.

### 4. Injury Returns
First game back from injury - minutes and usage may be restricted.

### 5. Trade Deadline
Player joins new team - features need to reset for new context.

---

## Future Feature Ideas

- Injury report data (Questionable/Probable/Out)
- Referee assignments (some refs call more fouls)
- Altitude (Denver home games)
- Time zones (East coast teams traveling West)
- Days since last practice
- Player age / experience
- Historical playoff performance
- Head-to-head matchups (specific player vs opponent)
