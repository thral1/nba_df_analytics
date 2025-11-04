"""
Head-to-head comparison of Linear Regression, Neural Networks, and XGBoost
All in SINGLE-STAGE mode (predicting total points directly)

Usage:
    python ml/compare_models_single_stage.py <db_path> <season>

Example:
    python ml/compare_models_single_stage.py data/nba.db 2018-19
"""

import sys
import sqlite3
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import xgboost as xgb

# Try to import TensorFlow/Keras, fallback to scikit-learn MLP
try:
    import tensorflow as tf
    from tensorflow import keras
    from tensorflow.keras import layers
    KERAS_AVAILABLE = True
    NN_BACKEND = "TensorFlow"
except (ImportError, Exception) as e:
    try:
        from sklearn.neural_network import MLPRegressor
        KERAS_AVAILABLE = True
        NN_BACKEND = "scikit-learn"
        print("NOTE: Using scikit-learn MLPRegressor instead of TensorFlow")
    except ImportError:
        print("WARNING: No neural network library available. NN comparison will be skipped.")
        KERAS_AVAILABLE = False
        NN_BACKEND = None

def load_data(db_path, season, season_type="regularseason"):
    """Load single-stage data (predicting total points directly)"""
    conn = sqlite3.connect(db_path)

    # Single-stage query: predict actual_PTS (total points)
    query = """
    SELECT
        PTS_mean, def_rtg_delta, off_rtg_v_position_delta, o_pts_delta,
        location_pts_effect, rest_effect, pts_paint_effect, pts_off_tov_effect,
        fb_effect, pts_2ndchance_effect, usg_pct, usg_pct_minus_tov,
        location_pts, rest_pts, opp_rest_pts, expected_PTS_pace, pts_pace_effect,
        expected_PTS_pace2, pts_pace2_effect, expected_PTS_pace3, pts_pace3_effect,
        expected_PTS_def_rtg, def_rtg_effect, expected_PTS_off_rtg_v_position,
        off_rtg_v_position_effect, expected_PTS_off_rtg, off_rtg_PTS_effect,
        expected_PTS_opp_PTS, expected_PTS_opp_PTS_effect, mean_starter_pts,
        mean_bench_pts, starterbench_pts_effect, mean_starterbench_pts,
        prev_pts, prev_pts_delta, prev2_pts, prev2_pts_delta, prev5_pts,
        prev5_pts_delta, ft_effect, expected_FTM, vegas_ratio_pts,
        vegas_ratio_pts_effect, vegas_ratio_pts_pinnacle,
        vegas_ratio_pts_effect_Pinnacle, vegas_ratio_pts_opp_pinnacle,
        vegas_ratio_pts_opp_effect_Pinnacle, vegas_ratio_pts_ou_pinnacle,
        vegas_ratio_pts_ou_effect_Pinnacle, adjusted_cfg_pts, adjusted_ufg_pts,
        adjusted_fg_pts, cfg_effect, b2b, opp_b2b, extra_rest, opp_extra_rest,
        starter, avg_mins_10_or_less, avg_mins_20_or_less, avg_mins_30_or_less,
        avg_mins_over_30, vegas_average_over_under_Pinnacle, over_under_ratio_pinnacle,
        point_spread_abs_3_or_less, point_spread_abs_6_or_less,
        point_spread_abs_9_or_less, point_spread_abs_12_or_less,
        point_spread_abs_over_9, point_spread_abs_over_12, point_spread_3_or_less,
        point_spread_6_or_less, point_spread_9_or_less, point_spread_12_or_less,
        point_spread_over_9, point_spread_over_12, point_spread_neg_3_or_less,
        point_spread_neg_6_or_less, point_spread_neg_9_or_less,
        point_spread_neg_12_or_less, point_spread_neg_over_9,
        point_spread_neg_over_12, pt_spread, playerNGame, seasonDoneRatio,
        actual_PTS
    FROM _{}_{}_XY
    """.format(season.replace('-', '_'), season_type)

    df = pd.read_sql_query(query, conn)
    conn.close()

    # Handle missing values
    df = df.fillna(0)

    # Separate features and target
    X = df.iloc[:, :-1]  # All columns except last
    y = df.iloc[:, -1]   # Last column (actual_PTS)

    print(f"Loaded {len(df)} observations from {season} {season_type}")
    print(f"Features: {X.shape[1]}, Target range: [{y.min():.2f}, {y.max():.2f}]")

    return X, y

def normalize_data(X_train, X_test):
    """Normalize features (needed for NN)"""
    train_stats = X_train.describe().transpose()

    def norm(x):
        return (x - train_stats['mean']) / train_stats['std']

    return norm(X_train), norm(X_test), train_stats

def train_linear_regression(X_train, X_test, y_train, y_test):
    """Train and evaluate Linear Regression"""
    print("\n" + "="*60)
    print("LINEAR REGRESSION (Single-Stage)")
    print("="*60)

    model = LinearRegression()
    model.fit(X_train, y_train)

    # Predictions
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)

    # Metrics
    results = {
        'model': 'Linear Regression',
        'train_mse': mean_squared_error(y_train, y_pred_train),
        'train_mae': mean_absolute_error(y_train, y_pred_train),
        'train_r2': r2_score(y_train, y_pred_train),
        'test_mse': mean_squared_error(y_test, y_pred_test),
        'test_mae': mean_absolute_error(y_test, y_pred_test),
        'test_rmse': np.sqrt(mean_squared_error(y_test, y_pred_test)),
        'test_r2': r2_score(y_test, y_pred_test)
    }

    print(f"Training   - MAE: {results['train_mae']:.3f}, R²: {results['train_r2']:.3f}")
    print(f"Testing    - MAE: {results['test_mae']:.3f}, RMSE: {results['test_rmse']:.3f}, R²: {results['test_r2']:.3f}")

    return results, model

def train_xgboost(X_train, X_test, y_train, y_test):
    """Train and evaluate XGBoost"""
    print("\n" + "="*60)
    print("XGBOOST (Single-Stage)")
    print("="*60)

    model = xgb.XGBRegressor(
        objective='reg:squarederror',
        colsample_bytree=0.3,
        learning_rate=0.1,
        max_depth=5,
        alpha=10,
        n_estimators=100,  # More trees for single-stage
        random_state=42
    )

    model.fit(X_train, y_train, verbose=False)

    # Predictions
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)

    # Metrics
    results = {
        'model': 'XGBoost',
        'train_mse': mean_squared_error(y_train, y_pred_train),
        'train_mae': mean_absolute_error(y_train, y_pred_train),
        'train_r2': r2_score(y_train, y_pred_train),
        'test_mse': mean_squared_error(y_test, y_pred_test),
        'test_mae': mean_absolute_error(y_test, y_pred_test),
        'test_rmse': np.sqrt(mean_squared_error(y_test, y_pred_test)),
        'test_r2': r2_score(y_test, y_pred_test)
    }

    print(f"Training   - MAE: {results['train_mae']:.3f}, R²: {results['train_r2']:.3f}")
    print(f"Testing    - MAE: {results['test_mae']:.3f}, RMSE: {results['test_rmse']:.3f}, R²: {results['test_r2']:.3f}")

    # Feature importance
    importance = model.feature_importances_
    top_features_idx = np.argsort(importance)[-10:]
    print(f"\nTop 10 most important features:")
    for idx in reversed(top_features_idx):
        print(f"  Feature {idx}: {importance[idx]:.4f}")

    return results, model

def train_neural_network(X_train, X_test, y_train, y_test, X_train_norm, X_test_norm):
    """Train and evaluate Neural Network"""
    if not KERAS_AVAILABLE:
        print("\n" + "="*60)
        print("NEURAL NETWORK - SKIPPED (No NN library available)")
        print("="*60)
        return None, None

    print("\n" + "="*60)
    print(f"NEURAL NETWORK (Single-Stage) - Using {NN_BACKEND}")
    print("="*60)

    if NN_BACKEND == "TensorFlow":
        # Build TensorFlow/Keras model
        model = keras.Sequential([
            layers.Dense(64, activation='relu', input_shape=[X_train.shape[1]]),
            layers.Dense(64, activation='relu'),
            layers.Dense(32, activation='relu'),
            layers.Dense(1)  # Output: single value (total points)
        ])

        model.compile(
            loss='mse',
            optimizer=tf.keras.optimizers.RMSprop(0.001),
            metrics=['mae', 'mse']
        )

        # Early stopping
        early_stop = keras.callbacks.EarlyStopping(monitor='val_loss', patience=20)

        # Train
        print("Training Neural Network (this may take a few minutes)...")
        history = model.fit(
            X_train_norm, y_train,
            epochs=500,
            validation_split=0.2,
            verbose=0,
            callbacks=[early_stop]
        )

        # Predictions
        y_pred_train = model.predict(X_train_norm, verbose=0).flatten()
        y_pred_test = model.predict(X_test_norm, verbose=0).flatten()

        epochs_trained = len(history.history['loss'])

    else:  # scikit-learn MLPRegressor
        # Build scikit-learn MLP model
        model = MLPRegressor(
            hidden_layer_sizes=(64, 64, 32),
            activation='relu',
            solver='adam',
            alpha=0.0001,
            learning_rate_init=0.001,
            max_iter=500,
            early_stopping=True,
            validation_fraction=0.2,
            n_iter_no_change=20,
            random_state=42,
            verbose=False
        )

        # Train
        print("Training Neural Network (this may take a few minutes)...")
        model.fit(X_train_norm, y_train)

        # Predictions
        y_pred_train = model.predict(X_train_norm)
        y_pred_test = model.predict(X_test_norm)

        epochs_trained = model.n_iter_

    # Metrics
    results = {
        'model': 'Neural Network',
        'train_mse': mean_squared_error(y_train, y_pred_train),
        'train_mae': mean_absolute_error(y_train, y_pred_train),
        'train_r2': r2_score(y_train, y_pred_train),
        'test_mse': mean_squared_error(y_test, y_pred_test),
        'test_mae': mean_absolute_error(y_test, y_pred_test),
        'test_rmse': np.sqrt(mean_squared_error(y_test, y_pred_test)),
        'test_r2': r2_score(y_test, y_pred_test)
    }

    print(f"Training   - MAE: {results['train_mae']:.3f}, R²: {results['train_r2']:.3f}")
    print(f"Testing    - MAE: {results['test_mae']:.3f}, RMSE: {results['test_rmse']:.3f}, R²: {results['test_r2']:.3f}")
    print(f"Iterations: {epochs_trained}")

    return results, model

def print_comparison(all_results):
    """Print comparison table"""
    print("\n" + "="*80)
    print("FINAL COMPARISON - SINGLE-STAGE PREDICTION (Total Points)")
    print("="*80)

    # Filter out None results
    results = [r for r in all_results if r is not None]

    if not results:
        print("No results to compare!")
        return

    print(f"\n{'Model':<20} {'Test MAE':<12} {'Test RMSE':<12} {'Test R²':<10} {'Winner'}")
    print("-" * 80)

    # Sort by MAE (lower is better)
    results_sorted = sorted(results, key=lambda x: x['test_mae'])

    for i, r in enumerate(results_sorted):
        winner = "*** BEST ***" if i == 0 else ""
        print(f"{r['model']:<20} {r['test_mae']:<12.3f} {r['test_rmse']:<12.3f} {r['test_r2']:<10.3f} {winner}")

    print("\nInterpretation:")
    print("  - MAE (Mean Absolute Error): Average points off by (lower is better)")
    print("  - RMSE (Root Mean Squared Error): Penalizes large errors more (lower is better)")
    print("  - R² (R-squared): Variance explained by model (higher is better, max 1.0)")

    # Show improvement
    if len(results_sorted) > 1:
        best = results_sorted[0]
        worst = results_sorted[-1]
        improvement = ((worst['test_mae'] - best['test_mae']) / worst['test_mae']) * 100
        print(f"\n{best['model']} beats {worst['model']} by {improvement:.1f}% (MAE)")

def main():
    if len(sys.argv) < 3:
        print("Usage: python compare_models_single_stage.py <db_path> <season>")
        print("Example: python compare_models_single_stage.py data/nba.db 2018-19")
        sys.exit(1)

    db_path = sys.argv[1]
    season = sys.argv[2]

    print("="*80)
    print("NBA PLAYER POINTS PREDICTION - MODEL COMPARISON")
    print("Single-Stage: Predicting Total Points Directly")
    print("="*80)
    print(f"Database: {db_path}")
    print(f"Season: {season}")

    # Load data
    X, y = load_data(db_path, season)

    # Train/test split (80/20)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    print(f"\nTrain set: {len(X_train)} observations")
    print(f"Test set:  {len(X_test)} observations")

    # Normalize for NN
    X_train_norm, X_test_norm, train_stats = normalize_data(X_train, X_test)

    # Train all models
    results = []

    # 1. Linear Regression
    lr_results, lr_model = train_linear_regression(X_train, X_test, y_train, y_test)
    results.append(lr_results)

    # 2. XGBoost
    xgb_results, xgb_model = train_xgboost(X_train, X_test, y_train, y_test)
    results.append(xgb_results)

    # 3. Neural Network
    nn_results, nn_model = train_neural_network(
        X_train, X_test, y_train, y_test, X_train_norm, X_test_norm
    )
    if nn_results:
        results.append(nn_results)

    # Print comparison
    print_comparison(results)

    print("\n" + "="*80)
    print("Comparison complete!")
    print("="*80)

if __name__ == "__main__":
    main()
