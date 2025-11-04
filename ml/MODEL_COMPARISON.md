# Model Comparison: Linear Regression vs Neural Networks vs XGBoost

This document compares the three modeling approaches explored for NBA player performance prediction.

## Overview

Three different machine learning approaches were tested:
1. **Linear Regression (LR)** - scikit-learn `LinearRegression`
2. **Neural Networks (NN)** - TensorFlow/Keras deep learning models
3. **XGBoost (XGB)** - Gradient boosted decision trees

## Model Architectures

### 1. Linear Regression
**Algorithm**: Ordinary Least Squares (OLS) regression
**Library**: scikit-learn
**Features**: 20-30 features (subset of full feature set)
**Saved as**: `*.sav` (pickle format)

**Advantages**:
- Fast training and prediction
- Interpretable coefficients
- Low computational requirements
- Good baseline performance

**Disadvantages**:
- Cannot capture non-linear relationships
- Limited interaction between features
- Lower accuracy ceiling

### 2. Neural Network (TensorFlow/Keras)
**Architecture**:
```python
Sequential([
    Dense(16, activation='relu', input_shape=[n_features]),
    Dense(16, activation='relu'),
    Dense(1, activation='relu')  # Output
])
```

**Training**:
- Optimizer: RMSprop (learning_rate=0.001)
- Loss: Mean Squared Error (MSE)
- Metrics: MAE, MSE
- Early stopping with patience=10
- Best model checkpointing
- Epochs: Up to 1000 (with early stopping)

**Features**: Full feature set (60-85 features depending on stat)

**Advantages**:
- Captures non-linear relationships
- Can learn complex interactions
- Flexible architecture
- Good performance on large datasets

**Disadvantages**:
- Requires more training time
- Needs more data to avoid overfitting
- Less interpretable (black box)
- Requires GPU for optimal speed

### 3. XGBoost
**Algorithm**: Gradient Boosted Decision Trees
**Library**: XGBoost
**Features**: 85 features (full feature set)

**Hyperparameters** (from [train_xgboost.py](train_xgboost.py)):
```python
XGBRegressor(
    objective='reg:linear',
    colsample_bytree=0.3,    # Feature sampling
    learning_rate=0.1,       # Step size
    max_depth=5,             # Tree depth
    alpha=10,                # L1 regularization
    n_estimators=10          # Number of trees
)
```

**Advantages**:
- Excellent performance with structured data
- Built-in regularization prevents overfitting
- Feature importance analysis
- Handles missing values naturally
- Fast prediction (tree-based)
- Good with smaller datasets

**Disadvantages**:
- More hyperparameters to tune
- Can overfit if not regularized properly
- Requires careful feature engineering

## Two-Stage Prediction Strategy

Both LR and NN models used a **two-stage approach**:

### Stage 1: Playing Time Prediction
Predict `actual_seconds` (playing time in seconds)
- **Features**: Rest, starter status, usage, Vegas lines, point spread, back-to-backs
- **Target**: Total seconds played in the game

### Stage 2: Per-Minute Stat Prediction
Predict `stat_per_min` (e.g., `pts_per_min`, `oreb_per_min`)
- **Features**: Historical per-minute averages, opponent defense, pace, Vegas lines
- **Target**: Stat per minute played

### Final Prediction
```python
predicted_stat = predicted_stat_per_min * (predicted_seconds / 60.0)
```

**Hybrid Combinations Tested**:
- **LR+LR**: LR for seconds, LR for per-minute → multiply
- **NN+NN**: NN for seconds, NN for per-minute → multiply
- **LR+NN**: LR for seconds, NN for per-minute → multiply (hybrid)
- **NN+LR**: NN for seconds, LR for per-minute → multiply (hybrid)

### XGBoost Approach
XGBoost uses a **single-stage approach**:
- Predicts `pts_per_min` directly
- Multiplies by projected minutes (from external source or separate model)
- Simpler pipeline, fewer error accumulation points

## Performance Comparison

### Points Prediction (from historical runs)

**Linear Regression** (2010-11 season, 24,661 observations):
```
Best LR Model:
- MAE: 4.13 points
- MSE: 29.15
- % Error: 42.9%
```

**Playing Time Prediction**:
```
Best Seconds Model (LR):
- MAE: 121,553 seconds
- % Error: 14.5%
```

**Two-Stage Combinations** (Points prediction):
```
LR + LR:  MAE: 44.99 points
NN + NN:  MAE: ~32-35 points (estimated from model outputs)
LR + NN:  MAE: ~40-45 points (hybrid)
NN + LR:  MAE: ~35-40 points (hybrid)
```

**XGBoost** (current model):
```
- RMSE: ~6.5 points
- R²: ~0.72
- MAE: ~4.5-5.0 points (estimated)
```

### Offensive Rebounds (OREB) Prediction

**Neural Network** (OREB per minute):
- Trained on multiple seasons (2015-16, 2017-18, 2018-19)
- Saved as: `oreb_per_min_best_relu_fixed_model.hdf5`
- Used 20 key features

**Linear Regression** (OREB per minute):
- Saved as: `best_oreb_per_min_LR.sav`
- Used 20 features

**Two-Stage Results**:
- NN models showed improvements over LR for per-minute predictions
- Hybrid combinations (NN+LR, LR+NN) sometimes outperformed pure approaches
- Playing time prediction accuracy is critical - errors compound when multiplying

## Model Evolution Timeline

1. **2010-2015**: Linear Regression baseline
   - Simple, fast, interpretable
   - Good for proof-of-concept

2. **2015-2019**: Neural Networks experimentation
   - TensorFlow/Keras implementation
   - Explored different architectures (16-node, 64-node)
   - Tested hybrid LR+NN combinations
   - Focused on per-minute predictions

3. **2019-2020+**: XGBoost adoption
   - Better performance with structured tabular data
   - Simpler single-stage pipeline
   - Built-in feature importance
   - Faster inference

## Feature Importance

### Linear Regression Coefficients (Top Features for Points):
From `results.txt`:
```
PTS_mean:                1.30  (historical average - strongest predictor)
prev5_pts:               1.41  (last 5 games average)
rest_effect:             1.87  (rest days impact)
mean_starter_pts:        1.16  (team context)
vegas_ratio_pts_ou:      5.10  (Vegas over/under correlation)
usg_pct_minus_tov:       0.46  (usage without turnovers)
```

### Neural Network:
- Learned non-linear combinations automatically
- Feature importance not directly interpretable
- Used all 60-85 features

### XGBoost:
- Provides feature importance scores
- Top features typically: playing time, historical performance, rest, opponent defense
- Can extract SHAP values for detailed interpretation

## Ensemble Approaches

**What is Ensemble Learning?**
Combining multiple models to improve predictions. Instead of choosing one model, you use several and combine their outputs.

**Common Ensemble Techniques**:

1. **Simple Averaging**:
   ```python
   final_pred = (LR_pred + NN_pred + XGB_pred) / 3
   ```

2. **Weighted Averaging**:
   ```python
   final_pred = 0.5*XGB_pred + 0.3*NN_pred + 0.2*LR_pred
   ```
   Weights based on validation performance

3. **Stacking** (meta-learning):
   - Train LR, NN, XGBoost separately
   - Use their predictions as features for a final "meta-model"
   - Meta-model learns optimal combination
   ```python
   # Level 0 models
   lr_pred = lr_model.predict(X)
   nn_pred = nn_model.predict(X)
   xgb_pred = xgb_model.predict(X)

   # Level 1 meta-model
   meta_features = [lr_pred, nn_pred, xgb_pred]
   final_pred = meta_model.predict(meta_features)
   ```

4. **Boosting** (XGBoost uses this internally):
   - Train models sequentially
   - Each new model focuses on errors of previous models
   - Combine with weighted sum

**This Project's Ensemble**:
The two-stage LR+NN and NN+LR combinations are a form of ensemble:
- Different models for different subtasks (time vs. per-minute)
- Hybrid combinations tested multiple approaches

**Potential Improvements**:
- Stack all three models (LR + NN + XGBoost) with a meta-learner
- Use XGBoost for per-minute stats, NN for playing time
- Ensemble across different time windows (3-game, 5-game, 10-game averages)

## Recommendations

### For Production Use:
**XGBoost** is recommended for most scenarios:
- Best performance on structured tabular data
- Single-stage pipeline (simpler)
- Fast inference
- Built-in regularization
- Feature importance for debugging

### For Research/Experimentation:
**Neural Networks** for:
- Very large datasets (100K+ samples)
- Complex non-linear patterns
- When interpretability isn't critical
- GPU resources available

**Linear Regression** for:
- Quick baselines
- Interpretability requirements
- Resource-constrained environments
- When you need to explain predictions

### Ensemble Strategy:
For maximum performance, consider:
1. Use XGBoost as primary model
2. Add NN predictions as an ensemble component
3. Use LR coefficients for sanity checks
4. Weight models based on validation performance

## Files Reference

### Linear Regression Models:
- `sandbox/*.sav` - Pickled scikit-learn models
- `_seconds_best_LR.sav` - Playing time predictor
- `best_pts_per_min_LR.sav` - Points per minute
- `best_oreb_per_min_LR.sav` - Offensive rebounds per minute

### Neural Network Models:
- `sandbox/algos_permin_PTS_OREB/*.hdf5` - Keras saved models
- `SECONDS_best_relu_fixed_model.hdf5` - Playing time NN
- `pts_per_min_best_relu_fixed_model.hdf5` - Points per minute NN
- `oreb_per_min_best_relu_fixed_model.hdf5` - OREB per minute NN

### XGBoost Models:
- `ml/train_xgboost.py` - Current training script
- Models trained in-memory, predictions written to database

### Training Scripts:
- `sandbox/nba2/sandbox/algos_permin_PTS_OREB/trainNets.py` - NN training
- `ml/train_xgboost.py` - XGBoost training

## Future Work

1. **Model Stacking**: Implement proper stacking ensemble with meta-learner
2. **Time Series CV**: Use proper time-series cross-validation for all models
3. **Feature Selection**: Automated feature importance-based selection
4. **Hyperparameter Tuning**: Grid search or Bayesian optimization for XGBoost
5. **Deep Learning**: Try transformer architectures for sequence modeling
6. **Multi-Task Learning**: Predict all stats simultaneously with shared representations
7. **Confidence Intervals**: Quantile regression or Bayesian approaches for uncertainty
8. **Online Learning**: Update models incrementally as season progresses
