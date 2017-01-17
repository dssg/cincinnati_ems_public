""" Before running IPython, run these two commands:
        export PYTHONPATH="/mnt/data/cincinnati/mkiang/cincinnati_ems/"
        export LUIGI_CONFIG_PATH=$PYTHONPATH"etl/pipeline/luigi.cfg"
    
    code_buckets:
    - high: 9, 19, 28, 10, 27, 14
    - medium: 12, 31, 13, 6, 8, 23, 15, 7, 24, 25,
    - low: 11, 2, 21, 3, 18, 16, 1, 20, 17, 26, 4, 29
    - other: all else
"""
bucket_map = {'9':'high', '19':'high', '28':'high', '10':'high', 
              '27':'high', '14':'high', 
              '12':'medium', '31':'medium', '13':'medium', '6':'medium', 
              '8':'medium', '23':'medium', '15':'medium', '7':'medium', 
              '24':'medium', '25':'medium',
              '11':'low', '2':'low', '21':'low', '3':'low', '18':'low', 
              '16':'low', '1':'low', '20':'low', '17':'low', '26':'low', 
              '4':'low', '29':'low'}

import os
import pickle
import matplotlib; matplotlib.use('Agg')
import pandas as pd
import numpy as np
import utils.pg_tools as pg
import matplotlib.pyplot as plt

PICKLE_DIR = '/mnt/data/cincinnati/model_pickle/'

## Make a connection
pgw = pg.PGWrangler(dbitems = pg.get_pgdict_from_cfg())
conn = pgw.get_conn()
model_scores = pd.read_sql_query('select incident, trns_to_hosp, code_type '
                                 'from semantic.master where time_year = 2015', 
                                 conn)
## convert bool to int
model_scores.trns_to_hosp = model_scores.trns_to_hosp + 0

## create index and remap buckets
model_scores.set_index('incident', inplace=True, drop=True)
model_scores['bucket'] = model_scores.code_type.map(bucket_map).fillna('other')

## Get pickle files
model_files = [PICKLE_DIR + x for x in os.listdir(PICKLE_DIR)]

## Loop and add to DF
for i, model_file in enumerate(model_files):
    try:
        print i, model_file
        temp = pickle.load(open(model_file, 'rb'))
        temp.set_index('incident', inplace=True, drop=True)
        temp.columns = ['m_' + model_file[-7:-2]]
        model_scores = pd.concat([model_scores, temp], axis=1)
    except:
        print i, "XX", model_file
        pass

## Subset
models_high = model_scores[model_scores.bucket == 'high'].copy()
models_med  = model_scores[model_scores.bucket == 'medium'].copy()
models_low  = model_scores[model_scores.bucket == 'low'].copy()
models_oth  = model_scores[model_scores.bucket == 'other'].copy()


## Exclude rows with mixed bucket models
def bool_vec(df, bucket='high'):
    bool_vec = df[df.bucket == bucket].count(axis=1) == max(df[df.bucket == 
                                                bucket].count(axis=1))
    return bool_vec

models_high = models_high[bool_vec(models_high, 
                            bucket='high')].dropna(axis=1).copy()
models_med  = models_med[bool_vec(models_med, 
                            bucket='medium')].dropna(axis=1).copy()
models_low  = models_low[bool_vec(models_low, 
                            bucket='low')].dropna(axis=1).copy()
models_oth  = models_oth[bool_vec(models_oth, 
                            bucket='other')].dropna(axis=1).copy()

models_high.to_csv('/mnt/data/cincinnati/model_scores_high.csv')
models_med.to_csv('/mnt/data/cincinnati/model_scores_medium.csv')
models_low.to_csv('/mnt/data/cincinnati/model_scores_low.csv')
models_oth.to_csv('/mnt/data/cincinnati/model_scores_other.csv')


def plot_precision_recall_n(y_true, y_prob, model_name):
    from sklearn.metrics import precision_recall_curve
    y_score = y_prob
    precision_curve, recall_curve, pr_thresholds = precision_recall_curve(y_true, y_score)
    precision_curve = precision_curve[:-1]
    recall_curve = recall_curve[:-1]
    pct_above_per_thresh = []
    number_scored = len(y_score)
    for value in pr_thresholds:
        num_above_thresh = len(y_score[y_score >= value])
        pct_above_thresh = num_above_thresh / float(number_scored)
        pct_above_per_thresh.append(pct_above_thresh)
    pct_above_per_thresh = np.array(pct_above_per_thresh)
    plt.clf()
    fig, ax1 = plt.subplots()
    ax1.plot(pct_above_per_thresh, precision_curve, 'b')
    ax1.set_xlabel('percent of population')
    ax1.set_ylabel('precision', color='b')
    ax1.set_ylim([0, 1])
    ax2 = ax1.twinx()
    ax2.set_ylim([0, 1])
    ax2.plot(pct_above_per_thresh, recall_curve, 'r')
    ax2.set_ylabel('recall', color='r')
    
    name = model_name
    plt.title(name)
    plt.savefig(name)
    # plt.show()


for col in models_high.columns[models_high.columns.str.startswith('m_')]:
    if os.path.isfile('./model_plots/high_' + col + '.png'):
        pass
    else:
        print col
        plot_precision_recall_n(models_high.trns_to_hosp, models_high[col], 
        'model_plots/high_' + col)
        plt.close('all')

for col in models_med.columns[models_med.columns.str.startswith('m_')]:
    if os.path.isfile('./model_plots/medium_' + col + '.png'):
        pass
    else:
        print col
        plot_precision_recall_n(models_med.trns_to_hosp, models_med[col], 
        'model_plots/medium_' + col)
        plt.close('all')

for col in models_low.columns[models_low.columns.str.startswith('m_')]:
    if os.path.isfile('./model_plots/low_' + col + '.png'):
        pass
    else:
        print col
        plot_precision_recall_n(models_low.trns_to_hosp, models_low[col], 
        'model_plots/low_' + col)
        plt.close('all')

for col in models_oth.columns[models_oth.columns.str.startswith('m_')]:
    if os.path.isfile('./model_plots/other_' + col + '.png'):
        pass
    else:
        print col
        plot_precision_recall_n(models_oth.trns_to_hosp, models_oth[col], 
        'model_plots/other_' + col)
        plt.close('all')
