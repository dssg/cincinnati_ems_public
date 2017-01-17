from __future__ import division
import matplotlib
matplotlib.use('Agg') 
import pandas as pd
import numpy as np
from sklearn import preprocessing, cross_validation, svm, metrics, tree, decomposition, svm
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier, GradientBoostingClassifier, AdaBoostClassifier
from sklearn.linear_model import LogisticRegression, Perceptron, SGDClassifier, OrthogonalMatchingPursuit, RandomizedLogisticRegression
from sklearn.neighbors.nearest_centroid import NearestCentroid
from sklearn.naive_bayes import GaussianNB, MultinomialNB, BernoulliNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
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

    clfs = {'RF': RandomForestClassifier(n_estimators=50, n_jobs=-1),
        'ET': ExtraTreesClassifier(n_estimators=10, n_jobs=-1, criterion='entropy'),
        'AB': AdaBoostClassifier(DecisionTreeClassifier(max_depth=1), algorithm="SAMME", n_estimators=200),
        'LR': LogisticRegression(penalty='l1', C=1e5),
        'SVM': svm.SVC(kernel='linear', probability=True, random_state=0),
        'GB': GradientBoostingClassifier(learning_rate=0.05, subsample=0.5, max_depth=6, n_estimators=10),
        'NB': GaussianNB(),
        'DT': DecisionTreeClassifier(),
        'SGD': SGDClassifier(loss="hinge", penalty="l2"),
        'KNN': KNeighborsClassifier(n_neighbors=3) 
            }

    grid = { 
        'RF':{'n_estimators': [10,100,1000,2500], 'max_depth': [10,20,50,100], 'max_features': ['sqrt','log2'],'min_samples_split': [2,5,10]},
        'LR': { 'penalty': ['l1','l2'], 'C': [.01,.1,1,10]},
    #'SGD': { 'loss': ['hinge','log','perceptron'], 'penalty': ['l2','l1','elasticnet']},
    'ET': { 'n_estimators': [10,100,1000,2500], 'criterion' : ['gini', 'entropy'] ,'max_depth': [10,20,50,100], 'max_features': ['sqrt','log2'],'min_samples_split': [2,5,10]},
    'AB': { 'algorithm': ['SAMME','SAMME.R'], 'n_estimators': [1,10,100]},
    'GB': {'n_estimators': [10,100,1000,2500], 'learning_rate' : [0.001,0.01,0.05,0.1,0.5],'subsample' : [0.1,0.5,1.0], 'max_depth': [5,10,20,50,100]},
    'NB' : {},
    'DT': {'criterion': ['gini', 'entropy'], 'max_depth': [1,5,10,20,50,100], 'max_features': ['sqrt','log2'],'min_samples_split': [2,5,10]},
    #'SVM' :{'C' :[0.01,0.1,1,10],'kernel':['linear']},
    'KNN' :{'n_neighbors': [1,5,10,25,50,100],'weights': ['uniform','distance'],'algorithm': ['auto','ball_tree','kd_tree']}
           }
    return clfs, grid

def clf_loop(models_to_run, clfs, grid, X_train, X_test, y_train, y_test, test_inc):
    list_of_results = []
    for n in range(1, 2):
        for index,clf in enumerate([clfs[x] for x in models_to_run]):
            parameter_values = grid[models_to_run[index]]
            for p in ParameterGrid(parameter_values):
                try:
                    clf.set_params(**p)
                    y_pred_probs = clf.fit(X_train, y_train).predict_proba(X_test)[:,1]
                    #df = pd.DataFrame(columns = ['incident', 'y_pred_probs'])
                    #df['incident'] = test_inc
                    #df['y_pred_probs'] = y_pred_probs
                    #pickle.dump( df, open( "result_df_labels.p", "wb" ) )
                    #pickle.dump( clf, open( "model.p", "wb" ) )
                    #plot_precision_recall_n(y_test,y_pred_probs,clf)
                    print clf
                    list_of_results.append((clf, metrics_at_k(y_test,y_pred_probs,.05), list(X_train.columns), datetime.datetime.now()))
                except IndexError, e:
                    print 'Error:',e
                    continue

    return list_of_results

def plot_precision_recall_n(y_true, y_prob, model_name):
    from sklearn.metrics import precision_recall_curve
    y_score = y_prob
    precision_curve, recall_curve, pr_thresholds = precision_recall_curve(y_true, y_score)
    precision_curve = precision_curve[:-1]
    recall_curve = recall_curve[:-1]
    pct_above_per_thresh = []
    number_scored = len(y_score)
    for value in pr_thresholds:
        num_above_thresh = len(y_score[y_score>=value])
        pct_above_thresh = num_above_thresh / float(number_scored)
        pct_above_per_thresh.append(pct_above_thresh)
    pct_above_per_thresh = np.array(pct_above_per_thresh)
    plt.clf()
    fig, ax1 = plt.subplots()
    ax1.plot(pct_above_per_thresh, precision_curve, 'b')
    ax1.set_xlabel('percent of population')
    ax1.set_ylabel('precision', color='b')
    ax1.set_ylim(0,1)
    ax2 = ax1.twinx()
    ax2.plot(pct_above_per_thresh, recall_curve, 'r')
    ax2.set_ylabel('recall', color='r')
    ax2.set_ylim(0,1)
    
    name = model_name
    plt.title(name)
    plt.tight_layout()
    plt.savefig('testpic.png')


def metrics_at_k(y_true, y_scores, k, weights):
    threshold = np.sort(y_scores)[::-1][int(k*len(y_scores))]
    y_pred = [int(i >= threshold) for i in y_scores]
    score_metric = 0
    zip_preds = zip(y_true, y_pred)
    
    score_metric += weights[0]*sum([i[0] + i[1] == 2 for i in zip_preds])
    score_metric += weights[1]*sum([i[0] == 1 and i[1] == 0 for i in zip_preds])
    score_metric += weights[2]*sum([i[0] == 0 and i[1] == 1 for i in zip_preds])
    score_metric += weights[3]*sum([i[0] + i[1] == 2 for i in zip_preds])
    return (metrics.precision_score(y_true, y_pred), metrics.recall_score(y_true, y_pred), metrics.roc_auc_score(y_true, y_scores), score_metric/float(len(y_true))) 

def run_all_models(train_df, test_df, features, models):

    #technical debt!!
    train_df['trns_to_hosp'] = train_df.trns_to_hosp.apply(lambda x: int(x) if x == 1 else 0)
    test_df['trns_to_hosp'] = test_df.trns_to_hosp.apply(lambda x: int(x) if x == 1 else 0)
    
    models_to_run = models
    clfs, grid = define_clfs_params()
    
    X_train = train_df[features]
    X_test = test_df[features]

    y_train = train_df.trns_to_hosp
    y_test = test_df.trns_to_hosp

    inc_test = test_df['incident']
    
    lor = clf_loop(models_to_run, clfs, grid, X_train, X_test, y_train, y_test, inc_test)
    return lor
