from sklearn.datasets import load_boston
import xgboost as xgb
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
import pandas as pd
import numpy as np

import sqlite3
import pdb
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
from pickle import dump
from pickle import load
import sys

'''
boston = load_boston()
print(boston.keys())
print(boston.data.shape)
print(boston.feature_names)
print(boston.DESCR)

data = pd.DataFrame(boston.data)
data.columns = boston.feature_names
data.head()
data['PRICE'] = boston.target
data.info()

X, y = data.iloc[:,:-1],data.iloc[:,-1]

data_dmatrix = xgb.DMatrix(data=X,label=y)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=123)

xg_reg = xgb.XGBRegressor(objective ='reg:linear', colsample_bytree = 0.3, learning_rate = 0.1,max_depth = 5, alpha = 10, n_estimators = 10)

xg_reg.fit(X_train,y_train)

preds = xg_reg.predict(X_test)

rmse = np.sqrt(mean_squared_error(y_test, preds))
print("RMSE: %f" % (rmse))

params = {"objective":"reg:linear",'colsample_bytree': 0.3,'learning_rate': 0.1,
                        'max_depth': 5, 'alpha': 10}

cv_results = xgb.cv(dtrain=data_dmatrix, params=params, nfold=3,
                            num_boost_round=50,early_stopping_rounds=10,metrics="rmse", as_pandas=True, seed=123)
cv_results.head()
print((cv_results["test-rmse-mean"]).tail(1))
'''


seasons = ["2017_18"]
seasons = ["2019_20"]
seasons = ["2015_16","2016_17","2017_18","2018_19"]
seasons = ["2018_19"]
seasons = ["2010_11","2011_12","2012_13","2013_14","2014_15","2015_16","2016_17","2017_18","2018_19"]
#seasons = ["2010_11","2011_12","2012_13","2013_14","2014_15","2015_16","2016_17","2017_18"]
seasons = ["2017_18","2018_19"]
seasons = ["2015_16","2017_18","2018_19"]
seasonType = "regularseason"
conn = sqlite3.connect(sys.argv[1])
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

dataset = pd.DataFrame( [] )
for season in seasons:
    print(season)
    #queryStr = "select PTS_mean, def_rtg_delta, off_rtg_v_position_delta, o_pts_delta, location_pts_effect, rest_effect, pts_paint_effect, pts_off_tov_effect, fb_effect, pts_2ndchance_effect, usg_pct, usg_pct_minus_tov, location_pts, rest_pts, opp_rest_pts, expected_PTS_pace, pts_pace_effect, expected_PTS_pace2, pts_pace2_effect, expected_PTS_pace3, pts_pace3_effect, expected_PTS_def_rtg, def_rtg_effect, expected_PTS_off_rtg_v_position, off_rtg_v_position_effect, expected_PTS_off_rtg, off_rtg_PTS_effect, expected_PTS_opp_PTS, expected_PTS_opp_PTS_effect, mean_starter_pts, mean_bench_pts, starterbench_pts_effect, mean_starterbench_pts, prev_pts, prev_pts_delta, prev2_pts, prev2_pts_delta, prev5_pts, prev5_pts_delta, ft_effect, expected_FTM, vegas_ratio_pts, vegas_ratio_pts_effect, vegas_ratio_pts_pinnacle, vegas_ratio_pts_effect_Pinnacle, vegas_ratio_pts_opp_pinnacle, vegas_ratio_pts_opp_effect_Pinnacle, vegas_ratio_pts_ou_pinnacle, vegas_ratio_pts_ou_effect_Pinnacle, adjusted_cfg_pts, adjusted_ufg_pts, adjusted_fg_pts, cfg_effect, b2b, opp_b2b, extra_rest, opp_extra_rest, starter, avg_mins_10_or_less,avg_mins_20_or_less,avg_mins_30_or_less,avg_mins_over_30,vegas_average_over_under_Pinnacle,over_under_ratio_pinnacle,point_spread_abs_3_or_less,point_spread_abs_6_or_less,point_spread_abs_9_or_less,point_spread_abs_12_or_less,point_spread_abs_over_9,point_spread_abs_over_12,point_spread_3_or_less,point_spread_6_or_less,point_spread_9_or_less,point_spread_12_or_less,point_spread_over_9,point_spread_over_12,point_spread_neg_3_or_less,point_spread_neg_6_or_less,point_spread_neg_9_or_less,point_spread_neg_12_or_less,point_spread_neg_over_9,point_spread_neg_over_12, pt_spread, playerNGame, seasonDoneRatio, actual_PTS from _{}_{}_XY".format(season,seasonType)
    queryStr = "select mean_pts_per_min,def_rtg_delta,off_rtg_v_position_delta,o_pts_delta_per_min,location_pts_effect_per_min,rest_effect_per_min,pts_paint_effect_per_min,pts_off_tov_effect_per_min,fb_effect_per_min,pts_2ndchance_effect_per_min,usg_pct,usg_pct_minus_tov,location_pts_per_min,rest_pts_per_min,opp_rest_pts_per_min,expected_PTS_pace_per_min,pts_pace_effect_per_min,expected_PTS_pace2_per_min,pts_pace2_effect_per_min,expected_PTS_pace3_per_min,pts_pace3_effect_per_min,expected_PTS_def_rtg_per_min,def_rtg_effect_per_min,expected_PTS_off_rtg_v_position_per_min,off_rtg_v_position_effect_per_min,expected_PTS_off_rtg_per_min,off_rtg_PTS_effect_per_min,expected_PTS_opp_PTS_per_min,expected_PTS_opp_PTS_effect_per_min,mean_starter_pts_per_min,mean_bench_pts_per_min,starterbench_pts_effect_per_min,starterbench_pts_effect_per_min,prev_pts_per_min,prev_pts_delta_per_min,prev2_pts_per_min,prev2_pts_delta_per_min,prev5_pts_per_min,prev5_pts_delta_per_min,ft_effect_per_min,expected_FTM_per_min,vegas_ratio_pts_per_min,vegas_ratio_pts_effect_per_min,vegas_ratio_pts_per_min_Pinnacle,vegas_ratio_pts_effect_per_min_Pinnacle,vegas_ratio_pts_opp_per_min_Pinnacle,vegas_ratio_pts_opp_effect_per_min_Pinnacle,vegas_ratio_pts_ou_per_min_Pinnacle,vegas_ratio_pts_ou_effect_per_min_Pinnacle,adjusted_cfg_pts_per_min,adjusted_ufg_pts_per_min,adjusted_fg_pts_per_min,cfg_effect_per_min,b2b,opp_b2b,extra_rest,opp_extra_rest,starter,avg_mins_10_or_less,avg_mins_20_or_less,avg_mins_30_or_less,avg_mins_over_30,vegas_average_over_under_Pinnacle,over_under_ratio_pinnacle,point_spread_abs_3_or_less,point_spread_abs_6_or_less,point_spread_abs_9_or_less,point_spread_abs_12_or_less,point_spread_abs_over_9,point_spread_abs_over_12,point_spread_3_or_less,point_spread_6_or_less,point_spread_9_or_less,point_spread_12_or_less,point_spread_over_9,point_spread_over_12,point_spread_neg_3_or_less,point_spread_neg_6_or_less,point_spread_neg_9_or_less,point_spread_neg_12_or_less,point_spread_neg_over_9,point_spread_neg_over_12,pt_spread,playerNGame,seasonDoneRatio,actual_pts_per_min from _{}_{}_XY".format(season,seasonType)
    tmpDF = pd.read_sql( queryStr, con = conn )
    dataset = dataset.append(tmpDF,ignore_index=True)

array = dataset.values
names = ['PTS_mean',  #0
        'def_rtg_delta', #1
        'off_rtg_v_position_delta', #2
        'o_pts_delta', #3
        'location_pts_effect', #4
        'rest_effect', #5
        'pts_paint_effect', #6
        'pts_off_tov_effect', #7
        'fb_effect', #8
        'pts_2ndchance_effect', #9
        'usg_pct',  #10
        'usg_pct_minus_tov',  #11
        'location_pts',  #12
        'rest_pts',  #13
        'opp_rest_pts',  #14
        'expected_PTS_pace',  #15
        'pts_pace_effect',  #16
        'expected_PTS_pace2',  #17
        'pts_pace2_effect',  #18
        'expected_PTS_pace3',  #19
        'pts_pace3_effect',  #20
        'expected_PTS_def_rtg',  #21
        'def_rtg_effect',  #22
        'expected_PTS_off_rtg_v_position',  #23
        'off_rtg_v_position_effect',  #24
        'expected_PTS_off_rtg',  #25
        'off_rtg_PTS_effect',  #26
        'expected_PTS_opp_PTS',  #27
        'expected_PTS_opp_PTS_effect',  #28
        'mean_starter_pts',  #29
        'mean_bench_pts',  #30
        'starterbench_pts_effect',  #31
        'mean_starterbench_pts',  #32
        'prev_pts',  #33
        'prev_pts_delta',  #34
        'prev2_pts',  #35
        'prev2_pts_delta',  #36
        'prev5_pts',  #37
        'prev5_pts_delta',  #38
        'ft_effect',  #39
        'expected_FTM',  #40
        'vegas_ratio_pts',  #41
        'vegas_ratio_pts_effect',  #42
        'vegas_ratio_pts_pinnacle',  #43
        'vegas_ratio_pts_pinnacle_effect',  #44
        'vegas_ratio_pts_opp_pinnacle',  #45
        'vegas_ratio_pts_opp_pinnacle_effect',  #46
        'vegas_ratio_pts_ou_pinnacle',  #47
        'vegas_ratio_pts_ou_pinnacle_effect',  #48
        'adjusted_cfg_pts',  #49
        'adjusted_ufg_pts',  #50
'adjusted_fg_pts',  #51
        'cfg_effect',  #52
        'b2b',  #53
        'opp_b2b',  #54
        'extra_rest',  #55
        'opp_extra_rest',  #56
        'starter',  #57
        'avg_mins_10_or_less', #58
        'avg_mins_20_or_less', #59
        'avg_mins_30_or_less', #60
        'avg_mins_over_30', #61
        'vegas_average_over_under_Pinnacle', #62
        'over_under_ratio_pinnacle', #63
        'point_spread_abs_3_or_less', #64
        'point_spread_abs_6_or_less', #65
        'point_spread_abs_9_or_less', #66
        'point_spread_abs_12_or_less', #67
        'point_spread_abs_over_9', #68
        'point_spread_abs_over_12', #69
        'point_spread_3_or_less', #70
        'point_spread_6_or_less', #71
        'point_spread_9_or_less', #72
        'point_spread_12_or_less', #73
        'point_spread_over_9', #74
        'point_spread_over_12', #75
        'point_spread_neg_3_or_less', #76
        'point_spread_neg_6_or_less', #77
        'point_spread_neg_9_or_less', #78
        'point_spread_neg_12_or_less', #79
        'point_spread_neg_over_9', #80
        'point_spread_neg_over_12', #81
        'pt_spread',#82
        'playerNGame',#83
        'seasonDoneRatio',#84
        'actual_PTS'] #85

names_pts_per_min = ['mean_pts_per_min',  #0
        'def_rtg_delta', #1 + 7952.36
        'off_rtg_v_position_delta', #2 -7952.99 (backout)
        'o_pts_delta_per_min', #3 + 7949.57
        'location_pts_effect_per_min', #4 7949.56
        'rest_effect_per_min', #5 ++ 7943.86
        'pts_paint_effect_per_min', #6 +7943.06
        'pts_off_tov_effect_per_min', #7 -7943.66 (backout)
        'fb_effect_per_min', #8 -7943.36(backout)
        'pts_2ndchance_effect_per_min', #9 -7943.10 (backout)
        'usg_pct',  #10 +++ 7909.15
        'usg_pct_minus_tov',  #11 + 7908.10 (7908.726 if backout #10)
        'location_pts_per_min',  #12 -double var
        'rest_pts_per_min',  #13 -double var
        'opp_rest_pts_per_min',  #14 -7908.34
        'expected_PTS_pace_per_min',  #15 +7907.88
        'pts_pace_effect_per_min',  #16 -double var
        'expected_PTS_pace2_per_min',  #17 +7901.07 (7903 if backout #15)
        'pts_pace2_effect_per_min',  #18 
        'expected_PTS_pace3_per_min',  #19 +7901.03 (7903 if backout #15, or 17) (7901.02 if backout #17)
        'pts_pace3_effect_per_min',  #20 -double var
        'expected_PTS_def_rtg_per_min',  #21 +7900.97
        'def_rtg_effect_per_min',  #22 -doublevar
        'expected_PTS_off_rtg_v_position_per_min',  #23 7900.81
        'off_rtg_v_position_effect_per_min',  #24 -doublevar
        'expected_PTS_off_rtg_per_min',  #25 nothing
        'off_rtg_PTS_effect_per_min',  #26  -doublevar
        'expected_PTS_opp_PTS_per_min',  #27 +7900.44
        'expected_PTS_opp_PTS_effect_per_min',  #28 -doublevar
        'mean_starter_pts_per_min',  #29 ++ 7864.90
        'mean_bench_pts_per_min',  #30 -7864.98
        'starterbench_pts_effect_per_min',  #31 - 7865
        'starterbench_pts_effect_per_min',  #32
        #'mean_starterbench_pts_per_min',  #32
        'prev_pts_per_min',  #33 7864.83
        'prev_pts_delta_per_min',  #34 -doublevar
        'prev2_pts_per_min',  #35 +7860.63
        'prev2_pts_delta_per_min',  #36 -doublevar
        'prev5_pts_per_min',  #37 ++7853.06
        'prev5_pts_delta_per_min',  #38 -doublevar
        'ft_effect_per_min',  #39 -7853.18 (backout)
        'expected_FTM_per_min',  #40 -7854 (backout)
        'vegas_ratio_pts_per_min',  #41 +7852.81
        'vegas_ratio_pts_effect_per_min',  #42 -doublevar
        'vegas_ratio_pts_per_min_Pinnacle',  #43 -doublevar
        'vegas_ratio_pts_effect_per_min_Pinnacle',  #44 -doublevar
        'vegas_ratio_pts_opp_per_min_Pinnacle',  #45 -7852.86
        'vegas_ratio_pts_opp_effect_per_min_Pinnacle',  #46 -doublevar
        'vegas_ratio_pts_ou_per_min_Pinnacle',  #47 +7850.99
        'vegas_ratio_pts_ou_effect_per_min_Pinnacle',  #48 -doublevar
        'adjusted_cfg_pts_per_min',  #49 ++7847.74
'adjusted_ufg_pts_per_min',  #50 7847.31
        'adjusted_fg_pts_per_min',  #51 -doublevar
        'cfg_effect_per_min',  #52 -doublevar
        'b2b',  #53 -7847.60
        'opp_b2b',  #54 +7847.44
        'extra_rest',  #55 7847.42
        'opp_extra_rest',  #56 7847.42
        'starter',  #57 -7851
        'avg_mins_10_or_less', #58 +7841.30
        'avg_mins_20_or_less', #59 +7836.49
        'avg_mins_30_or_less', #60 +7835.27
        'avg_mins_over_30', #61 - 7835.41
        'vegas_average_over_under_Pinnacle', #62 + (new 7833->7828.71) (old 7834.90)
        'over_under_ratio_pinnacle', #63 0change (new 7828.71->7828.72) (old +7834.76)
        'point_spread_abs_3_or_less', #64 -7835
        'point_spread_abs_6_or_less', #65 -7834.78
        'point_spread_abs_9_or_less', #66
        'point_spread_abs_12_or_less', #67
        'point_spread_abs_over_9', #68
        'point_spread_abs_over_12', #69
        'point_spread_3_or_less', #70
        'point_spread_6_or_less', #71
        'point_spread_9_or_less', #72
        'point_spread_12_or_less', #73
        'point_spread_over_9', #74
        'point_spread_over_12', #75
        'point_spread_neg_3_or_less', #76
        'point_spread_neg_6_or_less', #77
        'point_spread_neg_9_or_less', #78
        'point_spread_neg_12_or_less', #79
        'point_spread_neg_over_9', #80
        'point_spread_neg_over_12', #81
        'pt_spread',#82 -7835.72
        'playerNGame',#83 +7833.97
        'seasonDoneRatio',#84 +7832.61
        'actual_pts_per_min'] #85
'''
tests = [ ["mean_pts",names[0:1],array[:,0:1],array[:,-1]],
        ["cfg_pts",np.hstack((names[0:1],names[3:5],names[5:7],names[11:12],names[14:15],names[29:31],names[33:34],names[35:36],names[37:38],names[40:41],names[43:44],names[47:48],names[53:54],names[55:56],names[57:58],names[49:51],names[52:53],names[82:83],names[84:85])), np.hstack((array[:,0:1],array[:,3:5],array[:,5:7],array[:,11:12],array[:,14:15],array[:,29:31],array[:,33:34],array[:,35:36],array[:,37:38],array[:,40:41],array[:,43:44],array[:,47:48],array[:,53:54],array[:,55:56],array[:,57:58],array[:,49:51],array[:,52:53],array[:,82:83],array[:,84:85])),array[:,-1]],
        ]
'''
tests = [ ["mean_pts_per_min",names_pts_per_min[0:1],array[:,0:1],array[:,-1]],
        ["best_pts_per_min",np.hstack((names_pts_per_min[0:1],names_pts_per_min[1:2],names_pts_per_min[3:7],names_pts_per_min[10:12],names_pts_per_min[15:16],names_pts_per_min[19:20],names_pts_per_min[21:22],names_pts_per_min[23:24],names_pts_per_min[25:26],names_pts_per_min[27:28],names_pts_per_min[29:31],names_pts_per_min[33:34],names_pts_per_min[35:36],names_pts_per_min[37:38],names_pts_per_min[41:42],names_pts_per_min[47:48],names_pts_per_min[49:51],names_pts_per_min[58:61],names_pts_per_min[62:63],names_pts_per_min[83:85])),np.hstack((array[:,0:1],array[:,1:2],array[:,3:7],array[:,10:12],array[:,15:16],array[:,19:20],array[:,21:22],array[:,23:24],array[:,25:26],array[:,27:28],array[:,29:31],array[:,33:34],array[:,35:36],array[:,37:38],array[:,41:42],array[:,47:48],array[:,49:51],array[:,58:61],array[:,62:63],array[:,83:85])),array[:,-1]]
]
#'''

#pdb.set_trace()
'''
features.head(5)

print('The shape of our features is:', features.shape)
#The shape of our features is: (348, 9)
# Descriptive statistics for each column
features.describe()
# One-hot encode the data using pandas get_dummies
features = pd.get_dummies(features)# Display the first 5 rows of the last 12 columns
features.iloc[:,5:].head(5)

# Labels are the values we want to predict
labels = np.array(features['actual'])# Remove the labels from the features
# axis 1 refers to the columns
features = features.drop('actual', axis = 1)# Saving feature names for later use
feature_list = list(features.columns)# Convert to numpy array
features = np.array(features)
'''
X = features = 1*tests[1][2]
y = labels = 1*tests[1][3]

# Split the data into training and testing sets
#train_features, test_features, train_labels, test_labels = train_test_split(features, labels, test_size = 0.25, random_state = 42)
data_dmatrix = xgb.DMatrix(data=X,label=y)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=123)

xg_reg = xgb.XGBRegressor(objective ='reg:linear', colsample_bytree = 0.3, learning_rate = 0.1,max_depth = 5, alpha = 10, n_estimators = 10)

xg_reg.fit(X_train,y_train)

preds = xg_reg.predict(X_test)

mean_err = mean_squared_error(y_test,preds)
print("MSE: {}".format(mean_err))
mean_abs_err = mean_absolute_error(y_test,preds)
print("MAE: {}".format(mean_abs_err))
rmse = np.sqrt(mean_squared_error(y_test, preds))
print("RMSE: %f" % (rmse))

params = {"objective":"reg:linear",'colsample_bytree': 0.3,'learning_rate': 0.1,
        'max_depth': 5, 'alpha': 10}

cv_results = xgb.cv(dtrain=data_dmatrix, params=params, nfold=3,
        num_boost_round=50,early_stopping_rounds=10,metrics="rmse", as_pandas=True, seed=123)
cv_results.head()
print((cv_results["test-rmse-mean"]).tail(1))

#pdb.set_trace()

'''
gridsearch_params = [
    (max_depth, min_child_weight)
    for max_depth in range(9,12)
    for min_child_weight in range(5,8)
]
num_boost_round = 999
# Define initial best params and RMSE 
min_rmse = float("Inf")
best_params = None
for max_depth, min_child_weight in gridsearch_params:
    print("CV with max_depth={}, min_child_weight={}".format(max_depth,min_child_weight))    # Update our parameters
    params['max_depth'] = max_depth
    params['min_child_weight'] = min_child_weight    # Run CV
    cv_results = xgb.cv(
        params,
        data_dmatrix,
        num_boost_round=num_boost_round,
        seed=42,
        nfold=5,
        metrics={'rmse'},
        early_stopping_rounds=10
    )    # Update best RMSE
    mean_rmse = cv_results['test-rmse-mean'].min()
    boost_rounds = cv_results['test-rmse-mean'].argmin()
    print("\tRMSE {} for {} rounds".format(mean_rmse, boost_rounds))
    if mean_rmse < min_rmse:
        min_rmse = mean_rmse
        best_params = (max_depth,min_child_weight)
print("Best params: {}, {}, RMSE: {}".format(best_params[0], best_params[1], min_rmse))
pdb.set_trace()
'''

'''
params['max_depth'] = 9
params['min_child_weight'] = 7
gridsearch_params = [
    (subsample, colsample)
    for subsample in [i/10. for i in range(7,11)]
    for colsample in [i/10. for i in range(7,11)]
]

min_rmse = float("Inf")
best_params = None
# We start by the largest values and go down to the smallest
for subsample, colsample in reversed(gridsearch_params):
    print("CV with subsample={}, colsample={}".format(
                             subsample,
                             colsample))    
    # We update our parameters
    params['subsample'] = subsample
    params['colsample_bytree'] = colsample    
    # Run CV
    cv_results = xgb.cv(
        params,
        data_dmatrix,
        num_boost_round=num_boost_round,
        seed=42,
        nfold=5,
        metrics={'rmse'},
        early_stopping_rounds=10
    )    
    # Update best score
    mean_rmse = cv_results['test-rmse-mean'].min()
    boost_rounds = cv_results['test-rmse-mean'].argmin()
    print("\trmse {} for {} rounds".format(mean_rmse, boost_rounds))

    if mean_rmse < min_rmse:
        min_rmse = mean_rmse
        best_params = (subsample,colsample)

print("Best params: {}, {}, rmse: {}".format(best_params[0], best_params[1], min_rmse))
'''
'''
params['subsample'] = .9
params['colsample_bytree'] = 0.7
#%time# This can take some time…
min_rmse = float("Inf")
best_params = None
for eta in [.3, .2, .1, .05, .01, .005]:
    print("CV with eta={}".format(eta))    
    # We update our parameters
    params['eta'] = eta    
    # Run and time CV
    cv_results = xgb.cv(
            params,
	    data_dmatrix,
            num_boost_round=num_boost_round,
            seed=42,
            nfold=5,
            metrics=['rmse'],
            early_stopping_rounds=10
          )    
    # Update best score
    mean_rmse = cv_results['test-rmse-mean'].min()
    boost_rounds = cv_results['test-rmse-mean'].argmin()
    print("\trmse {} for {} rounds\n".format(mean_rmse, boost_rounds))
    if mean_rmse < min_rmse:
        min_rmse = mean_rmse
        best_params = eta

print("Best params: {}, rmse: {}".format(best_params, min_rmse))

pdb.set_trace()
'''

print("processing 2019-20")
dataset = pd.DataFrame( [] )
season = "2019_20"
seasons2 = ["2018_19","2019_20"]
seasons2 = ["2019_20"]
for season in seasons2:
    print(season)
#queryStr = "select PTS_mean, def_rtg_delta, off_rtg_v_position_delta, o_pts_delta, location_pts_effect, rest_effect, pts_paint_effect, pts_off_tov_effect, fb_effect, pts_2ndchance_effect, usg_pct, usg_pct_minus_tov, location_pts, rest_pts, opp_rest_pts, expected_PTS_pace, pts_pace_effect, expected_PTS_pace2, pts_pace2_effect, expected_PTS_pace3, pts_pace3_effect, expected_PTS_def_rtg, def_rtg_effect, expected_PTS_off_rtg_v_position, off_rtg_v_position_effect, expected_PTS_off_rtg, off_rtg_PTS_effect, expected_PTS_opp_PTS, expected_PTS_opp_PTS_effect, mean_starter_pts, mean_bench_pts, starterbench_pts_effect, mean_starterbench_pts, prev_pts, prev_pts_delta, prev2_pts, prev2_pts_delta, prev5_pts, prev5_pts_delta, ft_effect, expected_FTM, vegas_ratio_pts, vegas_ratio_pts_effect, vegas_ratio_pts_pinnacle, vegas_ratio_pts_effect_Pinnacle, vegas_ratio_pts_opp_pinnacle, vegas_ratio_pts_opp_effect_Pinnacle, vegas_ratio_pts_ou_pinnacle, vegas_ratio_pts_ou_effect_Pinnacle, adjusted_cfg_pts, adjusted_ufg_pts, adjusted_fg_pts, cfg_effect, b2b, opp_b2b, extra_rest, opp_extra_rest, starter, avg_mins_10_or_less,avg_mins_20_or_less,avg_mins_30_or_less,avg_mins_over_30,vegas_average_over_under_Pinnacle,over_under_ratio_pinnacle,point_spread_abs_3_or_less,point_spread_abs_6_or_less,point_spread_abs_9_or_less,point_spread_abs_12_or_less,point_spread_abs_over_9,point_spread_abs_over_12,point_spread_3_or_less,point_spread_6_or_less,point_spread_9_or_less,point_spread_12_or_less,point_spread_over_9,point_spread_over_12,point_spread_neg_3_or_less,point_spread_neg_6_or_less,point_spread_neg_9_or_less,point_spread_neg_12_or_less,point_spread_neg_over_9,point_spread_neg_over_12, pt_spread, playerNGame, seasonDoneRatio, actual_PTS from _{}_{}_XY".format(season,seasonType)
    queryStr = "select mean_pts_per_min,def_rtg_delta,off_rtg_v_position_delta,o_pts_delta_per_min,location_pts_effect_per_min,rest_effect_per_min,pts_paint_effect_per_min,pts_off_tov_effect_per_min,fb_effect_per_min,pts_2ndchance_effect_per_min,usg_pct,usg_pct_minus_tov,location_pts_per_min,rest_pts_per_min,opp_rest_pts_per_min,expected_PTS_pace_per_min,pts_pace_effect_per_min,expected_PTS_pace2_per_min,pts_pace2_effect_per_min,expected_PTS_pace3_per_min,pts_pace3_effect_per_min,expected_PTS_def_rtg_per_min,def_rtg_effect_per_min,expected_PTS_off_rtg_v_position_per_min,off_rtg_v_position_effect_per_min,expected_PTS_off_rtg_per_min,off_rtg_PTS_effect_per_min,expected_PTS_opp_PTS_per_min,expected_PTS_opp_PTS_effect_per_min,mean_starter_pts_per_min,mean_bench_pts_per_min,starterbench_pts_effect_per_min,starterbench_pts_effect_per_min,prev_pts_per_min,prev_pts_delta_per_min,prev2_pts_per_min,prev2_pts_delta_per_min,prev5_pts_per_min,prev5_pts_delta_per_min,ft_effect_per_min,expected_FTM_per_min,vegas_ratio_pts_per_min,vegas_ratio_pts_effect_per_min,vegas_ratio_pts_per_min_Pinnacle,vegas_ratio_pts_effect_per_min_Pinnacle,vegas_ratio_pts_opp_per_min_Pinnacle,vegas_ratio_pts_opp_effect_per_min_Pinnacle,vegas_ratio_pts_ou_per_min_Pinnacle,vegas_ratio_pts_ou_effect_per_min_Pinnacle,adjusted_cfg_pts_per_min,adjusted_ufg_pts_per_min,adjusted_fg_pts_per_min,cfg_effect_per_min,b2b,opp_b2b,extra_rest,opp_extra_rest,starter,avg_mins_10_or_less,avg_mins_20_or_less,avg_mins_30_or_less,avg_mins_over_30,vegas_average_over_under_Pinnacle,over_under_ratio_pinnacle,point_spread_abs_3_or_less,point_spread_abs_6_or_less,point_spread_abs_9_or_less,point_spread_abs_12_or_less,point_spread_abs_over_9,point_spread_abs_over_12,point_spread_3_or_less,point_spread_6_or_less,point_spread_9_or_less,point_spread_12_or_less,point_spread_over_9,point_spread_over_12,point_spread_neg_3_or_less,point_spread_neg_6_or_less,point_spread_neg_9_or_less,point_spread_neg_12_or_less,point_spread_neg_over_9,point_spread_neg_over_12,pt_spread,playerNGame,seasonDoneRatio,actual_pts_per_min from _{}_{}_XY".format(season,seasonType)
    tmpDF = pd.read_sql( queryStr, con = conn )
    dataset = dataset.append(tmpDF,ignore_index=True)

array = dataset.values
#'''
tests = [ ["mean_pts_per_min",names_pts_per_min[0:1],array[:,0:1],array[:,-1]],
        ["best_pts_per_min",np.hstack((names_pts_per_min[0:1],names_pts_per_min[1:2],names_pts_per_min[3:7],names_pts_per_min[10:12],names_pts_per_min[15:16],names_pts_per_min[19:20],names_pts_per_min[21:22],names_pts_per_min[23:24],names_pts_per_min[25:26],names_pts_per_min[27:28],names_pts_per_min[29:31],names_pts_per_min[33:34],names_pts_per_min[35:36],names_pts_per_min[37:38],names_pts_per_min[41:42],names_pts_per_min[47:48],names_pts_per_min[49:51],names_pts_per_min[58:61],names_pts_per_min[62:63],names_pts_per_min[83:85])),np.hstack((array[:,0:1],array[:,1:2],array[:,3:7],array[:,10:12],array[:,15:16],array[:,19:20],array[:,21:22],array[:,23:24],array[:,25:26],array[:,27:28],array[:,29:31],array[:,33:34],array[:,35:36],array[:,37:38],array[:,41:42],array[:,47:48],array[:,49:51],array[:,58:61],array[:,62:63],array[:,83:85])),array[:,-1]]
]
'''
tests = [ ["mean_pts",names[0:1],array[:,0:1],array[:,-1]],
        ["cfg_pts",np.hstack((names[0:1],names[3:5],names[5:7],names[11:12],names[14:15],names[29:31],names[33:34],names[35:36],names[37:38],names[40:41],names[43:44],names[47:48],names[53:54],names[55:56],names[57:58],names[49:51],names[52:53],names[82:83],names[84:85])), np.hstack((array[:,0:1],array[:,3:5],array[:,5:7],array[:,11:12],array[:,14:15],array[:,29:31],array[:,33:34],array[:,35:36],array[:,37:38],array[:,40:41],array[:,43:44],array[:,47:48],array[:,53:54],array[:,55:56],array[:,57:58],array[:,49:51],array[:,52:53],array[:,82:83],array[:,84:85])),array[:,-1]],
]
'''

X = features = 1*tests[1][2]
y = labels = 1*tests[1][3]

# Split the data into training and testing sets
#train_features, test_features, train_labels, test_labels = train_test_split(features, labels, test_size = 0.25, random_state = 42)
data_dmatrix = xgb.DMatrix(data=X,label=y)
preds = xg_reg.predict(X)
mean_err = mean_squared_error(y,preds)
print("MSE: {}".format(mean_err))
mean_abs_err = mean_absolute_error(y,preds)
print("MAE: {}".format(mean_abs_err))
rmse = np.sqrt(mean_squared_error(y, preds))
print("RMSE: %f" % (rmse))
pdb.set_trace()
