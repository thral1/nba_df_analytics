# Data Flow Documentation

## Overview

This document describes the complete data pipeline from raw data collection to ML predictions and lineup optimization.

## Pipeline Stages

### Stage 1: Data Collection

#### 1.1 NBA Box Scores
**Scripts**: `lib/scrapers/nba_boxscores.rb`

**Process**:
1. Query NBA.com API for daily scoreboard
2. For each game, fetch 7 different box score types:
   - Traditional (PTS, REB, AST, etc.)
   - Advanced (TS%, USG%, etc.)
   - Misc (PTS in paint, fast break points, etc.)
   - Scoring (2PT/3PT shooting splits)
   - Usage (touches, time of possession)
   - Four Factors (pace, ORB%, etc.)
   - Player Tracking (distance traveled, speed, etc.)
3. Save raw JSON responses
4. Parse into CSV files organized by season/game

**API Endpoints**:
```
http://stats.nba.com/stats/scoreboardV2?gameDate=MM%2FDD%2FYYYY
http://stats.nba.com/stats/boxscoretraditionalv2?GameID={id}&Season={season}
http://stats.nba.com/stats/boxscoreadvancedv2?GameID={id}&Season={season}
http://stats.nba.com/stats/boxscoremiscv2?GameID={id}&Season={season}
http://stats.nba.com/stats/boxscorescoringv2?GameID={id}&Season={season}
http://stats.nba.com/stats/boxscoreusagev2?GameID={id}&Season={season}
http://stats.nba.com/stats/boxscorefourfactorsv2?GameID={id}&Season={season}
http://stats.nba.com/stats/boxscoreplayertrackv2?GameID={id}&Season={season}
```

**Output**: CSV files in `data/raw/{season}/{seasontype}/`

#### 1.2 Betting Lines
**Scripts**: `lib/scrapers/betting_lines.rb`

**Process**:
1. Scrape historical betting lines from sports betting archives
2. Extract point spreads (opening and closing)
3. Extract over/under totals
4. Match to NBA game IDs
5. Store in database

**Data Fields**:
- Game date
- Home/away teams
- Point spread (home team perspective)
- Over/under total
- Sportsbook source

**Output**: Rows in `vegas_lines` table

#### 1.3 FanDuel Salaries
**Scripts**: `lib/scrapers/fanduel.rb`

**Process**:
1. Load FanDuel contest CSV exports
2. Parse player names, positions, salaries
3. Match players to NBA player IDs using fuzzy matching
4. Handle name variations and team changes

**Challenges**:
- Player name inconsistencies (e.g., "PJ Washington" vs "P.J. Washington")
- Mid-season trades
- Position designations differ from NBA official positions

**Output**: Rows in `fanduel_salaries` table

---

### Stage 2: Data Warehouse Loading

**Scripts**: `lib/database/loader.rb` (formerly `movetoDB.rb`)

**Process**:
1. Read CSV files from `data/raw/`
2. Infer column types (Integer, Float, String)
3. Create SQLite tables with dynamic schema
4. Insert rows with transaction batching for performance
5. Create indexes on key columns (GAME_ID, PLAYER_ID, DATE)

**Table Naming Convention**:
```
{season}_{seasontype}_{category}_{resultset}

Examples:
2023_24_regularseason_traditional_PlayerStats
2023_24_playoffs_advanced_PlayerStats
2023_24_regularseason_misc_TeamStats
```

**Performance Optimizations**:
- Batch inserts (1000 rows at a time)
- Disable auto-commit during bulk loads
- Create indexes after data load completes

---

### Stage 3: Feature Engineering

This is the most complex stage, transforming raw box scores into ML-ready features.

#### 3.1 Player Rolling Averages
**Scripts**: `lib/features/player_averages.rb`

**Process**:
```ruby
For each player in season:
  For each game (chronologically):
    1. Retrieve all prior games this season
    2. Calculate rolling statistics:
       - Mean (total / games_played)
       - Median (middle value of distribution)
       - Recent form (last 5 games)
    3. Track separate averages for:
       - Home vs Away
       - Back-to-back vs Normal rest
       - Starter vs Bench role
    4. Store in daily_averages table
```

**Example Calculations**:
```ruby
# Mean points per game
mean_PTS = total_PTS / games_played

# Points in last 5 games
recent_PTS = games[-5..-1].sum(:PTS) / 5

# Home/away splits
home_PTS = home_games.sum(:PTS) / home_games.count
away_PTS = away_games.sum(:PTS) / away_games.count
```

**Stats Tracked**:
- PTS, REB, OREB, DREB, AST, STL, BLK, TOV
- FGM, FGA, FG%, FG3M, FG3A, FG3%, FTM, FTA, FT%
- MIN (seconds played)
- Advanced: USG%, TS%, AST%, TOV%, etc.

#### 3.2 Rest Analysis
**Scripts**: `lib/features/player_averages.rb` (integrated)

**Rest Scenarios**:
```ruby
b2b = (current_game_date - previous_game_date) == 1
front_b2b = b2b && (next_game_date - current_game_date) == 1
extra_rest = (current_game_date - previous_game_date) >= 3
three_in_four = count_games_in_window(current_date, 4) >= 3
```

**Rest-Specific Averages**:
```ruby
mean_b2b_PTS = average(all_games.where(b2b: true).PTS)
mean_extra_rest_PTS = average(all_games.where(extra_rest: true).PTS)
```

**Impact**: Players typically score 10-15% fewer points on back-to-backs

#### 3.3 Matchup Features
**Scripts**: `lib/features/matchup_features.rb`

**Opponent Defense Metrics**:
```ruby
# Team defensive rating
o_team_def_rtg = opponent_average_def_rtg

# Position-specific defense
o_team_def_rtg_v_position = opponent_avg_def_rtg_vs_point_guards
```

**Expected Performance Adjustments**:
```ruby
# Adjust for opponent defense
league_avg_def_rtg = 110.0
def_rtg_ratio = league_avg_def_rtg / o_team_def_rtg
expected_PTS = player_mean_PTS * def_rtg_ratio

# Adjust for pace
league_avg_pace = 100.0
pace_ratio = game_pace / league_avg_pace
expected_PTS_pace = player_mean_PTS * pace_ratio
```

#### 3.4 Team Opponent Stats
**Scripts**: `lib/features/team_features.rb`

**Process**:
```ruby
For each team in season:
  For each position (PG, SG, SF, PF, C):
    Calculate opponent averages against this team's position:
      - Points allowed to position
      - Rebounds allowed
      - Assists allowed
      - Shooting percentages allowed

    Track both:
      - Season-long averages
      - Recent (last 10 games) averages
```

**Example Query**:
```sql
SELECT AVG(PTS) as avg_pts_allowed
FROM player_stats
WHERE opponent_team = 'LAL'
  AND player_position = 'PG'
  AND game_date < current_date
```

**Usage**: If Lakers allow 28 PPG to opposing point guards but league average is 24 PPG, we adjust predictions upward.

#### 3.5 Vegas Integration
**Scripts**: `lib/features/matchup_features.rb`

**Vegas-Based Features**:
```ruby
# Over/under implies expected total points
over_under = 220.5
team_pace = 102.3
league_pace = 100.0

# Estimate game pace
estimated_possessions = over_under / 2 / 1.10  # ~100 possessions

# Adjust player projections
pace_multiplier = estimated_possessions / team_avg_possessions
vegas_adjusted_PTS = player_mean_PTS * pace_multiplier
```

**Point Spread Features**:
```ruby
# Binary indicators
point_spread_abs_3_or_less = abs(spread) <= 3  # Close game
point_spread_abs_over_12 = abs(spread) > 12    # Blowout risk

# Blowouts affect playing time
if point_spread_abs_over_12:
  expected_minutes *= 0.85  # Starters may sit in 4th quarter
```

#### 3.6 Daily Team Averages
**Scripts**: `lib/features/team_features.rb`

**Process**:
```ruby
For each team in season:
  For each date:
    Calculate rolling team metrics:
      - Offensive rating
      - Defensive rating
      - Pace
      - Rebounding percentages
      - Turnover rates
      - Four factors

    Track league-wide averages for normalization
```

**Key Metrics**:
- Pace: Possessions per 48 minutes
- ORtg: Points per 100 possessions
- DRtg: Points allowed per 100 possessions
- eFG%: Effective field goal percentage
- TOV%: Turnover percentage
- ORB%: Offensive rebound percentage
- FTr: Free throw rate

---

### Stage 4: Feature Export

**Scripts**: `lib/features/feature_exporter.rb`

**Process**:
```ruby
For each game in database:
  For each player who played:
    Extract features from daily_averages table:
      1. Player historical averages (60+ features)
      2. Opponent defensive metrics (15+ features)
      3. Rest and schedule features (10+ features)
      4. Vegas features (10+ features)
      5. Situational features (10+ features)

    Append to CSV:
      [features..., actual_stat]

    Create separate CSV for each target:
      - points_{season}.csv
      - rebounds_{season}.csv
      - assists_{season}.csv
      - etc.
```

**Feature Vector Example** (Points prediction):
```csv
prev_mean_PTS, def_rtg_delta, o_pts_delta, b2b, opp_b2b, extra_rest, location,
pace_effect, expected_PTS_pace, expected_PTS_def_rtg, USG_PCT, starter,
mean_seconds, vegas_ratio_pts, point_spread_abs_3_or_less, actual_PTS

25.3, -2.1, 1.8, 0, 1, 0, 1, 1.05, 26.6, 24.1, 0.28, 1, 1980, 27.2, 1, 28
```

**Output Files**:
- `output/features/points_{season}.csv`
- `output/features/orebs_{season}.csv`
- `output/features/drebs_{season}.csv`
- `output/features/assists_{season}.csv`
- `output/features/steals_{season}.csv`
- `output/features/blocks_{season}.csv`
- `output/features/turnovers_{season}.csv`
- `output/features/seconds_{season}.csv`
- `output/features/points_per_min_{season}.csv`

**Row Count**: Typically 250,000+ rows per season (1,230 games × ~200 players per game)

---

### Stage 5: Machine Learning

**Scripts**: `ml/train_xgboost.py`, `ml/compare_models_single_stage.py`

#### 5.0 Model Selection

**Process**: Three modeling approaches were systematically compared on identical data:

1. **Linear Regression** (scikit-learn `LinearRegression`)
   - Fast baseline model
   - Highly interpretable coefficients
   - Assumes linear relationships

2. **XGBoost** (gradient boosted trees)
   - Handles non-linear relationships
   - Built-in regularization
   - Feature importance analysis

3. **Neural Networks** (TensorFlow/scikit-learn MLP)
   - Deep learning with 64-64-32 architecture
   - Captures complex patterns
   - Requires normalization

**Evaluation**: Head-to-head comparison on 4 NBA seasons (2017-21, 94,369 observations)

**Script**: `ml/compare_models_single_stage.py`
```bash
python ml/compare_models_single_stage.py sandbox/allXY.db 2018-19
```

**Results** (Mean Absolute Error across 4 seasons):
```
Model                Avg MAE    Seasons Won
------------------------------------------
Linear Regression    4.423      1/4 (2020-21)
XGBoost              4.440      3/4
Neural Network       4.454      0/4
```

**Key Findings**:
- All three models perform nearly identically (0.4% spread)
- XGBoost wins most seasons but only by 0.1-0.9%
- Feature engineering (85 features) matters more than algorithm choice
- ~50% variance is unexplained (R² ~0.52) - fundamental ceiling for NBA prediction

**Production Decision**: **XGBoost selected** because:
1. Best overall performance (wins 3/4 seasons)
2. Provides feature importance for debugging and interpretation
3. Fast inference (<1ms per prediction)
4. Built-in regularization prevents overfitting
5. Industry standard for structured tabular data

See [ml/EMPIRICAL_RESULTS.md](../ml/EMPIRICAL_RESULTS.md) for detailed comparison across all seasons.

---

#### 5.1 Data Loading
```python
import pandas as pd

# Load feature CSV
df = pd.read_csv('output/features/points_2023_24.csv')

# Separate features and target
X = df.iloc[:, :-1]  # All columns except last
y = df.iloc[:, -1]    # Last column (actual_PTS)

# Handle missing values
X = X.fillna(X.mean())
```

#### 5.3 Train/Test Split
```python
from sklearn.model_selection import TimeSeriesSplit

# Use time series split to prevent lookahead
tscv = TimeSeriesSplit(n_splits=5)

for train_idx, test_idx in tscv.split(X):
    X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
    y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
```

**Important**: Cannot use regular train_test_split because it would leak future information.

#### 5.4 Model Training
```python
import xgboost as xgb

# XGBoost regressor
model = xgb.XGBRegressor(
    n_estimators=1000,
    learning_rate=0.05,
    max_depth=7,
    min_child_weight=3,
    subsample=0.8,
    colsample_bytree=0.8,
    objective='reg:squarederror',
    early_stopping_rounds=50
)

model.fit(
    X_train, y_train,
    eval_set=[(X_test, y_test)],
    verbose=False
)
```

#### 5.5 Feature Importance
```python
import matplotlib.pyplot as plt

# Plot top 20 features
xgb.plot_importance(model, max_num_features=20)
plt.savefig('feature_importance.png')
```

**Typical Top Features** (Points):
1. `prev_mean_PTS` - Previous average points
2. `mean_seconds` - Average playing time
3. `USG_PCT` - Usage percentage
4. `expected_PTS_pace` - Pace-adjusted projection
5. `starter` - Starter vs bench
6. `expected_PTS_def_rtg` - Defense-adjusted projection

#### 5.6 Evaluation
```python
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

y_pred = model.predict(X_test)

rmse = mean_squared_error(y_test, y_pred, squared=False)
mae = mean_absolute_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print(f"RMSE: {rmse:.2f}")
print(f"MAE: {mae:.2f}")
print(f"R²: {r2:.3f}")
```

**Typical Performance**:
- Points RMSE: ~6.5 points
- Rebounds RMSE: ~2.1 rebounds
- Assists RMSE: ~1.8 assists

---

### Stage 6: Prediction Generation

**Scripts**: `ml/predict.py`

**Process**:
```python
# Load today's slate
slate_date = '2024-01-15'
games = get_todays_games(slate_date)

# For each player in today's games:
predictions = []
for player in get_players_in_games(games):
    # Extract features (same as training)
    features = extract_features(player, slate_date)

    # Generate predictions for all stats
    pred_pts = points_model.predict([features])[0]
    pred_reb = rebounds_model.predict([features])[0]
    pred_ast = assists_model.predict([features])[0]
    # ... etc

    predictions.append({
        'player_id': player.id,
        'player_name': player.name,
        'predicted_pts': pred_pts,
        'predicted_reb': pred_reb,
        'predicted_ast': pred_ast,
        # ... etc
    })

# Save predictions
pd.DataFrame(predictions).to_csv('predictions_2024_01_15.csv')
```

---

### Stage 7: Lineup Optimization

**Scripts**: `ml/optimization/lineup_optimizer.py`

**Problem**: Select 9 players (FanDuel) to maximize predicted fantasy points subject to:
- Budget constraint (salary cap $60,000)
- Position requirements (1 PG, 1 SG, 1 SF, 1 PF, 1 C, 1 G, 1 F, 1 UTIL, 1 UTIL)
- Max 4 players from same team
- Max 1 player from same game (for variance)

**Formulation** (Integer Linear Programming):
```python
from pulp import *

# Decision variables
x = {}
for player in players:
    x[player.id] = LpVariable(f"player_{player.id}", cat='Binary')

# Objective: Maximize fantasy points
prob = LpProblem("DFS_Lineup", LpMaximize)
prob += lpSum([predictions[p] * x[p] for p in players])

# Constraints
prob += lpSum([salaries[p] * x[p] for p in players]) <= 60000  # Budget

prob += lpSum([x[p] for p in players]) == 9  # Exactly 9 players

# Position constraints
prob += lpSum([x[p] for p in pg_eligible]) >= 1  # At least 1 PG
# ... etc for all positions

# Team exposure
for team in teams:
    prob += lpSum([x[p] for p in team.players]) <= 4

# Solve
prob.solve()

# Extract lineup
lineup = [p for p in players if x[p].varValue == 1]
```

**Output**:
```
PG: Stephen Curry (GSW) - $9,500 - Proj: 48.2 FP
SG: Devin Booker (PHX) - $8,800 - Proj: 44.1 FP
SF: Jayson Tatum (BOS) - $9,200 - Proj: 46.8 FP
...
Total Salary: $59,800
Total Projected: 412.5 FP
```

---

## Data Quality & Validation

### Missing Data Handling

**Common Issues**:
1. Player DNP (Did Not Play) - Minutes = 0 or NULL
2. Partial games (ejections, injuries)
3. API rate limiting
4. Historical data gaps

**Solutions**:
```ruby
# Skip DNP games
next if boxscore[:MIN].nil? || boxscore[:MIN] == "0:00"

# Handle missing advanced stats
if advanced_stats.nil?
  logger.warn("Missing advanced stats for game #{game_id}")
  use_derived_calculations()
end

# Retry on API failures
retry_count = 0
begin
  data = fetch_from_api(url)
rescue APIError => e
  retry_count += 1
  sleep(2 ** retry_count)  # Exponential backoff
  retry if retry_count < 3
  raise
end
```

### Data Validation

**Sanity Checks**:
```ruby
# Validate box score totals
if player_stats.sum(:PTS) != team_stats[:PTS]
  raise DataValidationError, "Points don't sum correctly"
end

# Check for impossible values
if boxscore[:MIN] > 3600  # >60 minutes
  raise DataValidationError, "Invalid minutes played"
end

# Ensure chronological order
if current_date < previous_date
  raise DataValidationError, "Dates out of order"
end
```

---

## Performance Considerations

### Bottlenecks

1. **Feature Calculation** - Most expensive operation
   - Must process 50K+ games sequentially
   - Each game requires querying all prior games
   - Optimization: Cache intermediate results

2. **Database Queries** - Can be slow without indexes
   - Solution: Index on (PLAYER_ID, DATE, GAME_ID)
   - Use database prepared statements

3. **API Rate Limiting** - NBA.com throttles requests
   - Solution: 2-second delay between requests
   - Batch processing overnight

### Optimization Techniques

```ruby
# Before: N+1 queries (SLOW)
players.each do |player|
  games = database[:boxscores].where(player_id: player.id).all
  # Process games...
end

# After: Single query with eager loading (FAST)
all_games = database[:boxscores].where(player_id: player_ids).all
games_by_player = all_games.group_by { |g| g[:player_id] }
players.each do |player|
  games = games_by_player[player.id]
  # Process games...
end
```

**Result**: 100x speedup (4 hours → 2.5 minutes)

---

## Timeline

**Full Pipeline Execution**:
- Data collection (1 season): ~6 hours
- Database loading: ~30 minutes
- Feature engineering: ~2 hours
- Model training: ~10 minutes per stat (9 stats = 90 min)
- Prediction generation: ~2 minutes
- Lineup optimization: ~30 seconds

**Total**: ~10 hours for one season end-to-end

**Incremental Updates** (daily):
- Fetch yesterday's games: ~5 minutes
- Update features: ~10 minutes
- Generate predictions: ~2 minutes

**Total**: ~20 minutes per day
