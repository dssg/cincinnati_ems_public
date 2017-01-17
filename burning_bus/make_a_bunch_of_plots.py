""""
export PYTHONPATH="/mnt/data/cincinnati/mkiang/cincinnati_ems/"
export LUIGI_CONFIG_PATH=$PYTHONPATH"etl/pipeline/luigi.cfg"
"""

import matplotlib; matplotlib.use('Agg')
import pandas as pd
import utils.pg_tools as pg
import seaborn as sns
import numpy as np
import hashlib
import json
import pickle
import matplotlib.pyplot as plt
import os
import errno

"""
code_buckets:
  - high: 9, 19, 28, 10, 27, 14, STRUCT, SHOOTF, SWAT, BOMB, FHELPF, 30, 22
  - medium: 12, 31, 13, 6, 8, 23, 15, 7, 24, 25, TRAP, TRAPF, COLAPS, WATERR, 
            SIG500, CUTF, PDOAF, HERONF, HEROIF, COSICK, DROWNF
  - low: 11, 2, 21, 3, 18, 16, 1, 20, 17, 26, 4, 29, PERDWF, GAS2, AIRF, 
         CHEMF, POSTAF, MUTUAL, ACCIF, ASSLTF, OUTDR, FUMES, SALV, SUICF, 
         WIRES, VEH, LOCK, WALKIN, CALARM, STUCK, INVEST, HYDR, BLDGF, 
         ROBBIF, TASER, GAS1, DOMNF, FRO, OUTLET, FTRACC, PHELPF, TRK, 
         RIVERF, RAPEF, CHEMI, BIOHZF, INACTF, HIRISK, DOMINF, MENTIF, 
         BOATF, EMS, FALARM, 32, 5
"""
bucket_map = {}
high = ['9', '19', '28', '10', '27', '14', 'STRUCT', 'SHOOTF', 'SWAT', 'BOMB', 
        'FHELPF', '30', '22']
med  = ['12', '31', '13', '6', '8', '23', '15', '7', '24', '25', 'TRAP', 
        'TRAPF', 'COLAPS', 'WATERR', 'SIG500', 'CUTF', 'PDOAF', 'HERONF', 
        'HEROIF', 'COSICK', 'DROWNF']
low  = ['11', '2', '21', '3', '18', '16', '1', '20', '17', '26', '4', '29', 
        'PERDWF', 'GAS2', 'AIRF', 'CHEMF', 'POSTAF', 'MUTUAL', 'ACCIF', 
        'ASSLTF', 'OUTDR', 'FUMES', 'SALV', 'SUICF', 'WIRES', 'VEH', 'LOCK', 
        'WALKIN', 'CALARM', 'STUCK', 'INVEST', 'HYDR', 'BLDGF', 'ROBBIF', 
        'TASER', 'GAS1', 'DOMNF', 'FRO', 'OUTLET', 'FTRACC', 'PHELPF', 'TRK', 
        'RIVERF', 'RAPEF', 'CHEMI', 'BIOHZF', 'INACTF', 'HIRISK', 'DOMINF', 
        'MENTIF', 'BOATF', 'EMS', 'FALARM', '32', '5']

for x in high:
    bucket_map[x] = 'high'

for x in med:
    bucket_map[x] = 'medium'

for x in low:
    bucket_map[x] = 'low'


def mkdir_p(path):
    """ If a directory does not exist, create it. """
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def make_json_df(m_table='model.lookup', k_values = [5, 30, 60, 90], 
                    return_raw=False, subset='2016-08-17'):
    """Just a wrapper to get the json info out of postgres    """
    ## Make a connection
    pgw = pg.PGWrangler(dbitems = pg.get_pgdict_from_cfg())
    conn = pgw.get_conn()

    ## Download the raw data -- clean up json column
    raw_data = pd.read_sql_query('select * from {}'.format(m_table), 
                                    conn)
    raw_data.columns = raw_data.columns.str.lower()
    raw_data.json = raw_data.json.apply(json.loads)

    if subset:
        raw_data = raw_data[raw_data.timestamp >= subset].copy()
        raw_data.reset_index(inplace=True, drop=True)

    # Helpful for debugging
    if return_raw:
        return raw_data
    
    ## Extract and normalize the json from raw data -- reindex
    json_df = pd.concat([pd.io.json.json_normalize(row[1]) for 
                            row in raw_data.json.iteritems()])
    json_df.reset_index(inplace=True, drop=True)

    ## Clean up column names
    json_df.columns = json_df.columns.str.lower()
    json_df.columns = json_df.columns.str.replace(' ', '_')
    json_df.columns = json_df.columns.str.replace('&_', '')

    ## Make 2 columns for feature and importance, listed alphabetically
    json_df['features_list_alpha'] = json_df.model_feats_imps.apply(
                                                lambda x: [y[0] for y in x])
    json_df['importance_list_alpha'] = json_df.model_feats_imps.apply(
                                                lambda x: [y[1] for y in x])

    ## Convert this list of lists into a dictionary -- easier handling
    json_df.model_feats_imps = json_df.model_feats_imps.apply(
                                            lambda x: {y[0]:y[1] for y in x})

    ## Now create a new features/importance colunm, sorted by abs(importance)
    json_df['features_sorted'] = json_df.model_feats_imps.apply(lambda x:
                                    sorted(x.items(), key=lambda (k, v): abs(v),
                                            reverse=True))

    ## Break out the precision, recall, AUC, weighted score
    for i, k in enumerate(k_values):
        json_df['p_' + str(k)] = json_df.model_metrics.apply(lambda x: 
                                                                x[i][1][0])
        json_df['r_' + str(k)] = json_df.model_metrics.apply(lambda x: 
                                                                x[i][1][1])
        json_df['auc_' + str(k)] = json_df.model_metrics.apply(lambda x: 
                                                                x[i][1][2])
        json_df['wsc_' + str(k)] = json_df.model_metrics.apply(lambda x: 
                                                                x[i][1][3])

    ## Hash the model and feature strings to make grouping easier
    json_df['feature_hash'] = json_df.features_list_alpha.apply(lambda x:
                                        hashlib.md5(' '.join(x)).hexdigest())
    json_df['model_hash'] = json_df.model_spec.apply(lambda x:
                                        hashlib.md5(' '.join(x)).hexdigest())
    
    ## change the weights and model spec to string to allow for grouping
    json_df['model_str'] = json_df.model_spec.apply(str)
    json_df['weights_str'] = json_df.weights.apply(str)

    json_df.model_str = json_df.model_str.str.replace('RandomForestClassifier', 'RF')
    json_df.model_str = json_df.model_str.str.replace('LogisticRegression', 'LR')
    json_df.model_str = json_df.model_str.str.replace('KNeighborsClassifier', 'KNN')
    json_df.model_str = json_df.model_str.str.replace('ExtraTreesClassifier', 'ET')
    json_df.model_str = json_df.model_str.str.replace('bootstrap=False, ', '')
    json_df.model_str = json_df.model_str.str.replace('class_weight=None, ', '')

    return json_df

def plot_precision_recall_n(y_true, y_prob, model_name, dir_name='./'):
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
    plt.savefig(dir_name + name)
    # plt.show()


#### START REAL STUFF
## THIS IS FOR THE TEST DATA
if os.path.isfile('./all_new_model_scores.csv'):
    model_scores = pd.read_csv('./all_new_model_scores.csv')
    model_scores.set_index('Unnamed: 0', inplace=True, drop=True)
    model_scores.index.rename('incident', inplace=True)
else:
    ## Get the truth
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

## THIS IS FOR THE TRAIN DATA
if os.path.isfile('./training_model_scores.csv'):
    train_scores = pd.read_csv('./training_model_scores.csv')
    train_scores.set_index('Unnamed: 0', inplace=True, drop=True)
    train_scores.index.rename('incident', inplace=True)
else:
    ## Get the truth
    pgw = pg.PGWrangler(dbitems = pg.get_pgdict_from_cfg())
    conn = pgw.get_conn()
    train_scores = pd.read_sql_query('select incident, trns_to_hosp, code_type '
                                    'from semantic.master where time_year = 2014'
                                    'or time_year = 2013', 
                                    conn)
    ## convert bool to int
    train_scores.trns_to_hosp = train_scores.trns_to_hosp + 0

    ## create index and remap buckets
    train_scores.set_index('incident', inplace=True, drop=True)
    train_scores['bucket'] = train_scores.code_type.map(bucket_map).fillna('other')

## Loop through all the pickle files we care about
models_df = make_json_df(return_raw=True)
# model_files = models_df.pickle.tolist()
model_files = models_df.pickle[models_df.timestamp > '2016-08-19 14:00'].tolist()


for i, model_file in enumerate(model_files):
    if ('m_' + model_file.split('/')[-1][:-2] 
        in model_scores.columns) and ('m_' + model_file.split('/')[-1][:-2] 
                                        in train_scores.columns):
        pass
    else:
        try:
            print i, model_file
            temp = pickle.load(open(model_file, 'rb'))
            if type(temp) is pd.DataFrame:
                temp.set_index('incident', inplace=True, drop=True)
                temp.columns = ['m_' + model_file[-34:-2]]
                model_scores = pd.concat([model_scores, temp], axis=1)
            else:
                train = temp['train']
                test = temp['test']

                ## do testing data first
                test.set_index('incident', inplace=True, drop=True)
                test.columns = ['m_' + model_file[-34:-2]]
                model_scores = pd.concat([model_scores, test], axis=1)

                ## then do training data
                train.set_index('incident', inplace=True, drop=True)
                train.columns = ['m_' + model_file[-34:-2]]
                train_scores = pd.concat([train_scores, train], axis=1)
        except:
            print i, "XX", model_file
            pass

model_scores.to_csv('./all_new_model_scores.csv')
train_scores.to_csv('./training_model_scores.csv')


for bucket in model_scores.bucket.unique():
    mkdir_p('./model_plots_new/' + bucket)

    current_slice = model_scores[model_scores.bucket == bucket]
    current_slice = current_slice.dropna(axis=1, how='all')

    for col in current_slice.columns[current_slice.columns.str.startswith('m_')]:
        model_slice = current_slice[['trns_to_hosp', col]].dropna(axis=0)
        model_name = bucket + " " + col + " " + str(len(model_slice))
        
        if os.path.isfile('./model_plots_new/' + bucket + 
                          '/' + model_name +  '.png'):
            pass
        else:
            print './model_plots_new/' + bucket + '/' + model_name
            plot_precision_recall_n(model_slice.trns_to_hosp, model_slice[col], 
                                    model_name=model_name, 
                                    dir_name='./model_plots_new/' + bucket + '/') 
        
        plt.close('all')

