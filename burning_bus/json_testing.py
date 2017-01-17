# export PYTHONPATH="/mnt/data/cincinnati/mkiang/cincinnati_ems/"
# export LUIGI_CONFIG_PATH=$PYTHONPATH"etl/pipeline/luigi.cfg"

import matplotlib; matplotlib.use('Agg')
import pandas as pd
import utils.pg_tools as pg
import seaborn as sns
import numpy as np
import hashlib
import json

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


def feature_extract(json_feature_row, feature_k=10):
    """ Helper function to extract some proportion of features """
    n_feat = len(json_feature_row) * feature_k/100
    top_k_feats = [x[0] for x in json_feature_row[:n_feat]]
    return top_k_feats


def summarize_model_table(json_df, metric, bucket=None, 
                          weights=None, model_k=80, feature_k=10):
    """ Summarizes a json_df created by make_json_df()

    :param json_df: pandas dataframe created by make_json_df()
    :param metric: string of the column we are interested in
    :param bucket: string of the bucket we are interested in
    :param weights: string representation of weight list
    :param model_k: int [0, 100] of the percentile cutoff of models to look at
    :param feature_k: int [0, 100] of the number of features to count
    """
    if bucket not in [None] + json_df.bucket_name.unique().tolist():
        raise Exception('Bucket not found')
    
    if metric not in json_df.columns:
        raise Exception('Metric not found')
    
    sub_df = json_df
    
    ## Subset to bucket we care about
    if bucket is not None:
        sub_df = sub_df[sub_df.bucket_name == bucket]
    
    ## Subset by weights
    if weights is not None:
        sub_df = sub_df[sub_df.weights_str == weights]

    ## Top performing features -- highest median across all models in bucket
    grp_df = sub_df.groupby('feature_hash')
    max_feature_hash = grp_df[metric].median().index[grp_df[metric].median() == 
                                            grp_df[metric].median().max()][0]
    top_feature_list = json_df.features_list_alpha[json_df.feature_hash == 
                                max_feature_hash].reset_index(drop=True)[0]
    
    ## Top performing model -- highest median across all features in bucket
    grp_df = sub_df.groupby('model_hash')
    max_model_hash = grp_df[metric].median().index[grp_df[metric].median() == 
                                            grp_df[metric].median().max()][0]
    top_model_spec = json_df.model_str[json_df.model_hash == 
                                    max_model_hash].reset_index(drop=True)[0]
    top_model_time = json_df.model_time[json_df.model_hash == 
                                    max_model_hash].reset_index(drop=True)[0]

    ## Top feature-model combination -- max metric
    best_model_feature = sub_df[sub_df[metric] == 
                                sub_df[metric].max()][['model_spec', 
                                                        'features_list_alpha']]
    best_model_time = sub_df[sub_df[metric] == 
                                sub_df[metric].max()]['model_time']

    ## Count of number of times a feature appears in the within the top k%
    ## of the top m% of models
    sub_models = sub_df[sub_df[metric] >= np.percentile(sub_df[metric], 
                                                            model_k)]
    feature_container = []
    for row in sub_models.features_sorted.iteritems():
        feature_container += feature_extract(row[1], feature_k=feature_k)
    feature_counts = {x:feature_container.count(x) for 
                                        x in set(feature_container)}
    sorted_features = sorted(feature_counts.items(), 
                                        key=lambda(k, v): abs(v), reverse=True)

    return {'top_features': top_feature_list, 
            'top_model': top_model_spec, 
            'overall_features': sorted_features, 
            'best_model_and_features': best_model_feature, 
            'best_model_features_time': best_model_time, 
            'top_model_time': top_model_time}

json_df = make_json_df()

buckets = json_df.bucket_name.unique().tolist()
weights = json_df.weights_str.unique().tolist()
metrics = ['auc_5', 'p_5', 'r_5', 'wsc_5', 'p_30', 'r_30', 'wsc_30',
           'p_60', 'r_60', 'wsc_60', 'p_90', 'r_90', 'wsc_90']

df_container = []
for bucket in buckets:
    for weight in weights:
        for metric in metrics:
            print bucket
            print weight
            print metric

            if len(json_df[metric][(json_df.weights_str == weight) & 
                                   (json_df.bucket_name == bucket)]) == 0:
                pass
            else:
                temp_df = pd.DataFrame({'weights':weight, 'bucket':bucket, 
                                        'metric':metric}, index=[0])

                temp_result = summarize_model_table(json_df, metric=metric, 
                                                    bucket=bucket, weights=weight)
                
                temp_df['top_model'] = temp_result['top_model']
                temp_df['top_features'] = ', '.join(temp_result['top_features'])
                temp_df['feat_count_top_k'] = ', '.join([x + ': ' + str(y) 
                                for x, y in temp_result['overall_features']])
                
                long_ass_string = ''
                for i, x in enumerate(temp_result['best_model_and_features']['model_spec']):
                    long_ass_string += x
                    long_ass_string += ': ' + ', '.join(temp_result['best_model_and_features']['features_list_alpha'].reset_index(drop=True)[i])
                
                temp_df['top_model_and_feature'] = long_ass_string

                temp_df['best_model_features_time'] = ', '.join(
                                    temp_result['best_model_features_time'])

                df_container.append(temp_df)

model_summaries = pd.concat(df_container)
model_summaries.reset_index(inplace=True, drop=True)

model_summaries.to_csv('/mnt/data/cincinnati/model_summaries_2.csv', index=False)