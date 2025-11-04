# NBA Fantasy Analytics Pipeline

Professional-grade NBA player performance prediction system for daily fantasy sports (DFS) optimization.

## Overview

This project implements an end-to-end analytics pipeline that:
- Collects NBA game data, betting lines, and DFS salaries from multiple sources
- Stores 10+ seasons of historical data in a SQLite data warehouse
- Engineers 60+ predictive features incorporating rest, matchups, pace, and defensive metrics
- Trains machine learning models to predict player performance across 9 statistical categories
- Optimizes DFS lineups using combinatorial optimization

## Features

### Data Collection
- **NBA.com API Integration**: Automated scraping of box scores (traditional, advanced, misc, scoring, usage, four factors, player tracking)
- **Betting Lines**: Vegas point spreads and over/under totals from multiple sportsbooks
- **DFS Salaries**: FanDuel salary data with player position mapping
- **Historical Coverage**: 2010-11 season through present

### Feature Engineering (60+ Features)
- **Player Metrics**: Rolling averages (mean, median) for all box score statistics
- **Rest Analysis**: Back-to-back games, extra rest, 3-games-in-4-nights
- **Matchup Features**: Opponent defensive rating, pace adjustments, position-specific defense
- **Situational Features**: Home/away splits, starter/bench designation, win percentage
- **Vegas Integration**: Point spread effects, over/under correlations
- **Pace Adjustments**: Expected performance based on game pace projections
- **Usage Metrics**: Usage percentage, touches, time of possession

### Machine Learning
- **Current Model**: XGBoost regressor for points-per-minute prediction
- **Experiments**: Linear Regression, Neural Networks (TensorFlow/Keras), XGBoost
- **Features**: 85 engineered features from historical performance, rest, matchups, and Vegas lines
- **Target**: Points per minute (scaled by projected playing time for total points)
- **Validation**: Time series train/test splits to prevent lookahead bias
- **Performance**: RMSE ~6.5 points, R² ~0.72 (XGBoost)
- **Comparison**: See [ml/MODEL_COMPARISON.md](ml/MODEL_COMPARISON.md) for detailed benchmarks

### Lineup Optimization
- **Algorithm**: Integer linear programming (ILP) using Google OR-Tools (SCIP solver)
- **Constraints**: Salary cap ($60,000), position requirements (1C, 2PF, 2SF, 2SG, 2PG = 9 players)
- **Objective**: Maximize total projected fantasy points
- **Solve Time**: <1 second to global optimality
- **Output**: 10 diverse lineups per day

## Architecture

```
Data Sources                Data Warehouse              Feature Engineering           ML Pipeline
┌─────────────┐            ┌──────────────┐            ┌─────────────────┐          ┌──────────────┐
│  NBA.com    │            │              │            │ Player Rolling  │          │   Training   │
│  API        │──────────> │    SQLite    │──────────> │   Averages      │────────> │              │
│             │            │   Database   │            │                 │          │   XGBoost    │
│ Vegas Lines │──────────> │              │──────────> │ Matchup Stats   │────────> │  Regressor   │
│             │            │  32+ Seasons │            │                 │          │              │
│ FanDuel     │──────────> │  50K+ Games  │            │ Situational     │          │ Predictions  │
│ Salaries    │            │              │            │   Features      │          │              │
└─────────────┘            └──────────────┘            └─────────────────┘          └──────┬───────┘
                                                                                            │
                                                                                            v
                                                                                   ┌──────────────┐
                                                                                   │  Lineup      │
                                                                                   │  Optimizer   │
                                                                                   │ (OR-Tools)   │
                                                                                   └──────────────┘
```

## Project Structure

```
nba-fantasy-analytics/
├── lib/                    # Core Ruby library
│   ├── scrapers/          # Data collection from external sources
│   ├── parsers/           # Data parsing and transformation
│   ├── models/            # Domain models (Player, Team, Game, StatSet)
│   ├── database/          # Database access and schema
│   ├── features/          # Feature engineering modules
│   └── utils/             # Helper functions and utilities
├── ml/                     # Python machine learning pipeline
│   ├── train_xgboost.py  # XGBoost model training and prediction
│   ├── optimize_lineups.py # DFS lineup optimizer (OR-Tools ILP)
│   ├── analyze_lineups.py  # Lineup performance analysis
│   └── README.md          # ML pipeline documentation
├── config/                 # Configuration files
│   ├── seasons.yml        # Season dates and metadata
│   └── database.yml       # Database configuration
├── spec/                   # RSpec tests
├── bin/                    # Executable scripts
├── data/                   # Data storage (gitignored)
│   ├── raw/               # Downloaded CSVs
│   └── processed/         # SQLite database
└── output/                 # Generated files (gitignored)
    └── features/          # Feature CSVs for ML
```

## Technology Stack

### Data Pipeline (Ruby)
- **Ruby 3.0+**: Core scripting language
- **Sequel**: Database ORM
- **Nokogiri**: HTML/XML parsing
- **Optimist**: Command-line argument parsing
- **RSpec**: Testing framework

### Machine Learning (Python)
- **Python 3.10+**: ML pipeline language
- **pandas**: Data manipulation
- **scikit-learn**: ML framework and evaluation metrics
- **XGBoost**: Gradient boosting regressor
- **OR-Tools**: Google's optimization library (ILP solver)

### Data Storage
- **SQLite**: Relational database (portable, no server required)

## Installation

### Prerequisites
- Ruby 3.0 or higher
- Python 3.10 or higher
- SQLite 3

### Ruby Setup
```bash
# Install dependencies
bundle install
```

### Python Setup
```bash
# Install ML dependencies
pip install -r requirements.txt
```

## Usage

### Data Collection
```bash
# Fetch box scores for a season
bin/fetch_boxscores --season 2023-24 --type regularseason

# Download betting lines
bin/fetch_betting_lines --season 2023-24 --start-date 2023-10-24

# Get FanDuel salaries
bin/fetch_fanduel_salaries --date 2024-01-15
```

### Feature Engineering
```bash
# Calculate features for a season
bin/process_features --season 2023-24 --output output/features/

# This generates:
# - points_2023_24.csv
# - rebounds_2023_24.csv
# - assists_2023_24.csv
# - etc.
```

### Machine Learning

See [ml/README.md](ml/README.md) for detailed documentation.

```bash
# Train XGBoost model and generate predictions
python ml/train_xgboost.py data/processed/nba.db 2019-20 regularseason

# Optimize DFS lineups
python ml/optimize_lineups.py data/processed/nba.db 2019-20 2020-01-15

# Analyze lineup performance
python ml/analyze_lineups.py
```

## Data Sources

### NBA.com Stats API
- Box scores (7 different stat types per game)
- Player gamelogs
- Team statistics
- Historical data back to 1991-92 season

**Endpoints Used**:
- `/stats/scoreboardV2` - Daily game schedules
- `/stats/boxscoretraditionalv2` - Traditional box scores
- `/stats/boxscoreadvancedv2` - Advanced metrics
- `/stats/boxscoremiscv2` - Miscellaneous stats
- And 4 more specialized endpoints

### Betting Lines
- Historical point spreads
- Over/under totals
- Opening and closing lines
- Multiple sportsbooks for consensus

### FanDuel
- Daily player salaries
- Position eligibility
- Salary trends over time

## Key Features Explained

### Rest-Based Features
- `b2b`: Binary indicator for back-to-back games
- `front_b2b`: First game of back-to-back
- `extra_rest`: 3+ days rest
- `mean_b2b_PTS`: Average points in back-to-back situations
- `mean_extra_rest_PTS`: Average points with extra rest

### Matchup Features
- `o_team_def_rtg`: Opponent defensive rating
- `league_average_def_rtg`: League average for normalization
- `expected_PTS_def_rtg`: Expected points based on opponent defense
- `o_team_def_rtg_v_position`: Opponent defense vs player's position

### Pace Features
- `expected_PTS_pace`: Expected points based on game pace
- `team_mean_pace`: Team's average pace
- `opponent_mean_pace`: Opponent's average pace

### Usage Features
- `USG_PCT`: Usage percentage
- `USG_PCT_minus_TOV`: Usage minus turnover impact
- `mean_AST_RATIO`: Assist-to-turnover ratio

### Vegas Features
- `vegas_ratio_pts`: Expected points based on over/under
- Point spread indicators (various thresholds)

## Model Performance

(To be updated with actual metrics after training)

- **Points RMSE**: TBD
- **Rebounds RMSE**: TBD
- **Assists RMSE**: TBD

## Development

### Testing

**Current Status**: Basic unit tests exist for utility functions (`spec/lib/utils/`). Integration tests for the full pipeline are planned but not yet implemented.

```bash
# Run existing Ruby tests
bundle exec rspec
```

**Future Work**: Add integration tests for:
- Data scraping and parsing
- Feature engineering calculations
- ML model predictions
- Lineup optimization

## Database Schema

### Key Tables
- `{season}_{type}_traditional_PlayerStats`: Basic box scores
- `{season}_{type}_daily_averages`: Calculated rolling averages and features
- `vegas_lines`: Betting data
- `fanduel_salaries`: DFS salary information
- `rosters`: Player-team mappings

See `docs/DATABASE_SCHEMA.md` for complete schema documentation.

## Configuration

### Season Configuration
Edit `config/seasons.yml` to add new seasons:
```yaml
seasons:
  2024-25:
    regular_season_start: "2024-10-22"
    regular_season_end: "2025-04-13"
    playoffs_end: "2025-06-20"
```

### Database Configuration
Set database path via environment variable:
```bash
export DB_PATH=data/processed/nba.db
```

## Contributing

This is a personal project, but suggestions and feedback are welcome via GitHub issues.

## Known Limitations

- Relies on NBA.com API which is unofficial and may change
- Does not include injury data (future enhancement)
- Betting line coverage varies by season (more complete in recent years)
- FanDuel salary data only available from 2013-14 season onward

## Future Enhancements

- [ ] Add injury data integration
- [ ] Implement neural network models
- [ ] Add real-time data updates
- [ ] Create web dashboard for predictions
- [ ] Multi-site DFS optimization (DraftKings, Yahoo)
- [ ] Ensemble model stacking

## License

MIT License - See LICENSE file for details

## Acknowledgments

- NBA.com for providing statistical data
- Sports betting sites for historical line data
- Basketball-Reference.com for supplemental player information

## Contact

For questions or collaboration opportunities, please open a GitHub issue.

---

**Note**: This project is for educational and research purposes. Always gamble responsibly and follow local laws and regulations.
