# Machine Learning & Optimization

This directory contains the Python machine learning and optimization components of the NBA Fantasy Analytics pipeline.

## Overview

The ML pipeline consists of three main components:

1. **`train_xgboost.py`** - XGBoost regression model for player performance prediction
2. **`optimize_lineups.py`** - Integer linear programming optimizer for DFS lineup generation
3. **`analyze_lineups.py`** - Lineup performance analysis and comparison tool

**Note**: This project explored three different modeling approaches (Linear Regression, Neural Networks, XGBoost). See [MODEL_COMPARISON.md](MODEL_COMPARISON.md) for detailed comparison and performance benchmarks.

**Run Your Own Comparison**: Use `compare_models_single_stage.py` to test all three models head-to-head on your data:
```bash
python ml/compare_models_single_stage.py <db_path> <season>
# Example: python ml/compare_models_single_stage.py data/nba.db 2018-19
```

## Data Flow

```
parsedata.rb → Database (_${season}_${type}_XY tables)
                    ↓
            train_xgboost.py (predictions)
                    ↓
            Database (results_${season} tables)
                    ↓
            optimize_lineups.py (optimal lineups)
                    ↓
            FanDuel-ready lineup output
```

## Setup

Install Python dependencies:

```bash
pip install -r ../requirements.txt
```

## Usage

### 1. Train XGBoost Model & Generate Predictions

```bash
python ml/train_xgboost.py <db_path> <season> <season_type>
```

**Arguments:**
- `db_path` - Path to SQLite database (e.g., `data/processed/nba.db`)
- `season` - Season in format `YYYY-YY` (e.g., `2019-20`)
- `season_type` - Either `regularseason` or `playoffs`

**Example:**
```bash
python ml/train_xgboost.py data/processed/nba.db 2019-20 regularseason
```

**What it does:**
- Loads training data from `_${season}_${seasonType}_XY` table
- Trains XGBoost regressor on 85 features to predict points per minute
- Performs time series train/test split
- Evaluates model with RMSE, MAE, R² metrics
- Generates predictions for upcoming games
- Writes predictions to `results_${season}` table

**Model Performance:**
- RMSE: ~6.5 points per game
- R²: ~0.72 (explains 72% of variance)
- Top features: playing time, historical performance, rest, opponent defense

### 2. Optimize DFS Lineups

```bash
python ml/optimize_lineups.py <db_path> <season> <date>
```

**Arguments:**
- `db_path` - Path to SQLite database
- `season` - Season in format `YYYY-YY`
- `date` - Date in format `YYYY-MM-DD`

**Example:**
```bash
python ml/optimize_lineups.py data/processed/nba.db 2019-20 2020-01-15
```

**What it does:**
- Loads predictions from `results_${season}` table for the specified date
- Formulates integer linear programming problem using OR-Tools
- Objective: Maximize total projected fantasy points
- Constraints:
  - Salary cap: $60,000
  - Position requirements: 1 C, 2 PF, 2 SF, 2 SG, 2 PG (9 players total)
- Solves to global optimality in <1 second
- Generates 10 diverse lineups by iteratively excluding top players

**Output:**
```
Lineup #1 (Projected: 312.5 pts, Salary: $59,800):
  PG: Stephen Curry ($9,500) - 52.3 pts
  PG: Damian Lillard ($9,200) - 48.7 pts
  SG: James Harden ($11,500) - 58.2 pts
  ...
```

### 3. Analyze Lineup Performance

```bash
python ml/analyze_lineups.py
```

**What it does:**
- Compares your lineup predictions vs. FantasyCruncher (FC) projections
- Analyzes actual performance for a date range
- Reports median and max scores
- Shows which projection system performed better

**Use case:** Validate your model's performance against commercial alternatives

## Database Schema

### Input Table: `_${season}_${seasonType}_XY`

Contains 85 columns:
- 84 feature columns (engineered by `parsedata.rb`)
- 1 target column: `actual_pts_per_min`

**Key features:**
- Rolling averages (points, assists, rebounds, etc.)
- Rest metrics (days since last game, back-to-back indicator)
- Opponent defense (defensive rating vs. position)
- Pace adjustments (possessions per game)
- Vegas lines (over/under, spread)
- Matchup history
- Usage rate trends

### Output Table: `results_${season}`

Contains predictions for each player-game:
- `player_name`, `player_id`, `date`
- `pos` - Position (PG/SG/SF/PF/C)
- `fd_salary` - FanDuel salary
- `projected_fdscore` - Predicted fantasy points
- `actual_fdscore` - Actual fantasy points (filled post-game)
- `projected_seconds` - Predicted playing time
- `actual_seconds` - Actual playing time

## Technical Details

### XGBoost Model

```python
xg_reg = xgb.XGBRegressor(
    objective='reg:linear',      # Regression task
    colsample_bytree=0.3,        # Feature sampling
    learning_rate=0.1,           # Step size
    max_depth=5,                 # Tree depth (prevents overfitting)
    alpha=10,                    # L1 regularization
    n_estimators=10              # Number of trees
)
```

**Why XGBoost?**
- Handles non-linear relationships well (sports data is highly non-linear)
- Built-in regularization prevents overfitting
- Feature importance analysis reveals key predictors
- Fast training and prediction

### Optimization Formulation

**Decision Variables:**
- `x_i ∈ {0,1}` for each player `i` (1 = selected, 0 = not selected)

**Objective Function:**
```
Maximize: Σ(projected_fdscore_i * x_i)
```

**Constraints:**
```
Salary:     Σ(fd_salary_i * x_i) ≤ 60,000
Centers:    Σ(x_i | pos_i = C) = 1
PFs:        Σ(x_i | pos_i = PF) = 2
SFs:        Σ(x_i | pos_i = SF) = 2
SGs:        Σ(x_i | pos_i = SG) = 2
PGs:        Σ(x_i | pos_i = PG) = 2
Total:      Σ(x_i) = 9
```

**Solver:** Google OR-Tools with SCIP (Solving Constraint Integer Programs)
- Guarantees global optimality
- Solves in <1 second for typical slate sizes

## Future Enhancements

- Add LightGBM for comparison
- Implement ensemble stacking (combine multiple models)
- Add injury data integration
- Multi-objective optimization (maximize ceiling, minimize risk)
- Real-time model monitoring for drift detection
- Web dashboard for visualization
