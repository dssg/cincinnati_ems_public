from __future__ import division
import matplotlib
matplotlib.use('Agg') 
import pandas as pd
import numpy as np
from sklearn import preprocessing, cross_validation, svm, metrics, tree, decomposition, svm

from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet, SGDRegressor, Lars, LassoLars, BayesianRidge, Perceptron


from sklearn.cross_validation import train_test_split
from sklearn.grid_search import ParameterGrid
from sklearn.metrics import *
from sklearn.preprocessing import StandardScaler
import random
import pylab as pl
import matplotlib.pyplot as plt
from scipy import optimize
import time
import datetime
import pickle

def define_clfs_params():

    clfs = {'RIDGE': Ridge(),
            'LINEAR': LinearRegression(),
            'LASSO': Lasso(),
            'ELNET': ElasticNet(),
            'LARS': Lars(),
            'LLARS': LassoLars(),
            'BRIDGE': BayesianRidge(),
            'PER': Perceptron()
       
            }

    grid = {
        'RIDGE': {'alpha': [0.01,0.1,1,10]},
        'LINEAR':  {},
        'LASSO':  {'alpha': [0.01,0.1,1,10]},
        'ELNET':  {'alpha': [0.01,0.1,1,10], 'l1_ratio':[0.25,0.5,0.75]},
        'LARS':  {},
        'LLARS':  {},
        'BRIDGE':  {},
        'PER':  {}
       
           }
    return clfs, grid



def reg_metrics(y_true, y_pred, final_weight = 0.1, schedule = 'exp', overunder =[1,1]):
    mape = np.mean([abs(item[1] - item[0])/float(item[0]) for item in zip(y_true, y_pred) if item[0] != 0])
    mse = metrics.mean_squared_error(y_true, y_pred)**0.5
    mae = metrics.mean_absolute_error(y_true, y_pred)
    
    if schedule == 'exp':
        k = np.log(1/float(final_weight))/(len(y_true)-1)
        tweights = [np.exp(-k*i) for i in range(len(y_true))]
    elif schedule == 'lin':
        tweights = [(i+1)*(final_weight-1)/float(len(y_true)) + 1 for i in range(len(y_true))]
    
    twscore = np.mean([((overunder[0]-overunder[1])*(int(item[0] < item[1])) + overunder[1])
                       *(abs(item[1] - item[0])/float(item[0]))
                       *(tweights[item[2]])
                       for item in zip(y_true, y_pred, range(len(y_pred))) if item[0] != 0])
    
    return (mape, mse, mae, twscore)
