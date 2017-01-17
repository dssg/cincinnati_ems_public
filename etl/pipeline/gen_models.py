# coding utf-8
# Luigi Imports
import luigi
import luigi.postgres
from luigi import configuration, LocalTarget
from sklearn.preprocessing import Imputer, StandardScaler
# External Imports
import run_models
import pandas as pd
import numpy as np
import datetime
import pickle
import os
from utils.pg_tools import PostgresTask, PGTableTarget, CreateSchema
from utils.pg_tools import FileOlderTarget
from create_semantic import CreateSemanticTable
from sklearn.grid_search import ParameterGrid
import json
import hashlib
import re

# Set Up Logging
import logging
logging.config.fileConfig('/mnt/data/cincinnati/cincinnati_ems_logging.conf')
logger = logging.getLogger('cincinnati')

from etl.pipeline.feature_creation import CreateFeatureTable

# Generate Sample Data
class SampleData(PostgresTask):

    # Parameter values are found in luigi.cfg
    initial_train_year = luigi.IntParameter()
    train_set_numyear = luigi.IntParameter()
    initial_test_year = luigi.IntParameter()
    test_set_numyear = luigi.IntParameter()

    schema_feats = luigi.Parameter()
    train_data_feats = luigi.Parameter()

    schema_model = luigi.Parameter()
    tbl_train = luigi.Parameter()
    tbl_test = luigi.Parameter()
    feat_types = luigi.Parameter()

    model_specs_file = luigi.Parameter()
    success_file = luigi.Parameter()

    def requires(self):
        # We need the semantic table to exist and the model schema to exist
        return [CreateFeatureTable(),
                CreateSchema(self.schema_model)]

    def output(self):
        # we will output the training and test tables
        yield PGTableTarget(table=self.tbl_train, schema=self.schema_model)
        yield PGTableTarget(table=self.tbl_test, schema=self.schema_model)
        yield [FileOlderTarget(old_file=self.model_specs_file,
                                young_file=self.success_file)
               ]

    def run(self):

        # Get features table
        conn = self.pgw.get_conn()
        feature_df = pd.read_sql_query('select * from {}.{}'.format(
            self.schema_feats, self.train_data_feats), con=conn)
        data_types_df = pd.read_sql_query('select * from {}.{}'.format(
            self.schema_feats, self.feat_types), con=conn)
        conn.close()

        # categorical variable binary generation
        categorize = []
        for i, row in data_types_df.iterrows():
            if row['type'] == 'cat':
                categorize.append(row['name'])

        # drop one of the binary columns to avoid multicolinearity
        for col in categorize:
            dummy_hour = pd.get_dummies(feature_df[col], prefix=col + '_is')
            feature_df = feature_df.join(dummy_hour)
            if not (col == 'code_type'):
                tot = [i for i in feature_df.columns if col + '_is' in i]
                feature_df = feature_df.drop(tot[0], 1)

        # Load train table into database
        train_df = feature_df[feature_df['time_year'].apply(
            lambda x: x >= self.initial_train_year
            and x < self.initial_train_year + self.train_set_numyear)]
        test_df = feature_df[feature_df['time_year'].apply(
            lambda x: x >= self.initial_test_year
            and x < self.initial_test_year + self.test_set_numyear)]

        # drop the original categorical column
        for col in categorize:
            train_df = train_df.drop(col, 1)
            test_df = test_df.drop(col, 1)

        # Load test table into database
        pk = 'incident'
        train_df.reset_index(inplace=True)
        train_df.set_index(pk, inplace=True)
        train_df.drop('index', axis=1, inplace=True)
        self.pgw.df_to_pg(train_df, self.tbl_train, self.schema_model,
                          if_exists='replace', pk=pk)
        test_df.reset_index(inplace=True)
        test_df.set_index(pk, inplace=True)
        test_df.drop('index', axis=1, inplace=True)
        self.pgw.df_to_pg(test_df, self.tbl_test, self.schema_model,
                          if_exists='replace', pk=pk)

        self.pgw.create_timestamp(self.success_file)

# Calculate Probability of Transport
class RunModels(PostgresTask):

    # Get luigi parameters from luigi.cfg file
    models_used = luigi.Parameter()
    features = luigi.Parameter()
    success_file = luigi.Parameter()
    model_specs_file = luigi.Parameter()
    code_buckets = luigi.Parameter(default=" - all: all_the_codes")
    label = luigi.Parameter()
    other_bucket = luigi.BoolParameter()

    def requires(self):
        # get models to use
        models_to_run = [x.strip() for x in self.models_used.split('-')[1:]]
        buckets = [(name, 
                    [y.strip() for y in codes.split(',')]) 
                    for (name, codes) in [x.strip().split(':') for x in 
                                          self.code_buckets.split('-')[1:]
                                         ]
                  ]
        # Construct union of all codes for computing it's compliment for 
        # 'other' in RunML
        all_codes = []
        all_bucket = False
        for (name, codes) in buckets:
            if name=='all':
                all_bucket = True
            else:
                all_codes += codes
        if (not(all_bucket and len(buckets) == 1) and self.other_bucket):
            buckets.append(('other',all_codes))

        clfs, grid = run_models.define_clfs_params()

        # Wrapper loop for running all models
        runs = []
        counter = 0
        for index, clf in enumerate([clfs[x] for x in models_to_run]):
            parameter_values = grid[models_to_run[index]]
            for p in ParameterGrid(parameter_values):
                for b in buckets:
                    runs.append(RunML(p, clf, features=self.features,
                                  model_counter=counter, code_bucket=b,
                                  label=self.label))
                    counter += 1

        return runs

    def output(self):
        # The output will be the table of each model and a lookup table with
        # metadata of the models
        return [FileOlderTarget(old_file=self.model_specs_file,
                                young_file=self.success_file%self.label)
               ]

    def run(self):
        self.pgw.create_timestamp(self.success_file%self.label)


class RunML(PostgresTask):
    imputing_hack = luigi.BoolParameter(default=False)

    tbl_train = luigi.Parameter()
    tbl_test = luigi.Parameter()
    tbl_results = luigi.Parameter()
    tbl_all_info = luigi.Parameter()

    success_file = luigi.Parameter()

    schema_model = luigi.Parameter()
    tbl_all_info = luigi.Parameter()

    storage_path_json = luigi.Parameter()
    storage_path_pkl = luigi.Parameter()

    label = luigi.Parameter()
    key = luigi.Parameter()

    model_specs_file = luigi.Parameter()
    
    model_counter = luigi.Parameter()
    features = luigi.Parameter()
    code_bucket = luigi.Parameter()

    code_weights = luigi.Parameter()

    def __init__(self, p, clf, *args, **kwargs):
        super(RunML, self).__init__(*args, **kwargs)

        self.clf = clf
        self.p = p
        columns = self.pgw.get_column_names(
            table_name=self.tbl_train, schema=self.schema_model)
        # get features
        feature_classes = [i for i in self.features.replace(
            '\n', '').split('- ') if i != '']

        # Code bucketing
        features = set([i for e in feature_classes for i in columns if e in i])
        features_nocodes = features - set([x for x in features 
                                           if 'code_type' in x])
        codes = set([x[14:-1] for x in features if 'code_type' in x])
        (bucket_name, bucket_codes) = self.code_bucket
        bucket_codes = set(bucket_codes) - set([''])
        if not bucket_name == 'all':
            if self.code_bucket[0] == 'other':
                codes -= bucket_codes
            else:
                codes = bucket_codes
        features = list(set(columns) & (features_nocodes 
                        | set(['"code_type_is_%s"'%x for x in codes])))

        weight_tups = []
        weight_str = re.findall(r'-.*[0-9].*[0-9].*[0-9].*[0-9]', self.code_weights)
        for item in weight_str:
            l = (item.strip('-').split(':'))
            weights = [float(i) for i in l[1].split(' ')[1:]]
            weight_tups.append((l[0].strip(), weights))

        weight_dict = {}
        for item in weight_tups:
            weight_dict[item[0]] = item[1]
        
        self.inc_weights = weight_dict
        features.sort()
        codes = list(codes)
        codes.sort()

        self.bucket_codes = codes
        self.bucket_name = bucket_name

        self.features = [x.strip().replace('"', '') for x in features]

        self.feat_str = (', '.join(features) + ', ' + '"' + self.label +'"' 
                         + ', ' + '"' + self.key + '"')
 
    def requires(self):
        return SampleData()

    def output(self):
        return FileOlderTarget(old_file=self.model_specs_file,
                young_file=self.success_file % self.hash_name())

    def hash_name(self):
        # Feat_str has features, key, and label
        # Code bucket implicitly in features
        return hashlib.md5("".join(self.feat_str)
                           + str(self.clf.__class__)
                           + str(self.p)
                           + str(self.inc_weights)
                          ).hexdigest()

    def run(self):
        # we need for the train and test data to exist
        # load train and test tables
        # change * to features to be used
        conn = self.pgw.get_conn()
        train_df = pd.read_sql_query('select {} from {}.{}'.format(
            self.feat_str, self.schema_model, self.tbl_train),
            con=conn)
        train_df.set_index(self.key, inplace=True)
        test_df = pd.read_sql_query('select {} from {}.{}'.format(
            self.feat_str, self.schema_model, self.tbl_test),
            con=conn)
        conn.close()
        test_df.set_index(self.key, inplace=True)

        lookup_tbl = pd.DataFrame(columns=['TIMESTAMP', 'ID', 'JSON', 'PICKLE'])
        
        code_feats = [i for i in self.features if 'code_type_is_' in i]
        scaler = StandardScaler()
        imputer = Imputer(strategy='median')

        # Scale all columns that are not boolean (len>3 inl. None)
        # and that are not the label
        scale_cols = [x for x in train_df.columns if 
                      ((not x is self.label)
                       and (len(train_df[x].unique())>=3))]
        # Subset
        train_df = train_df[train_df[code_feats].sum(axis=1) == 1]
        # Avoid multicoliniarity
        train_df.drop(code_feats[0], axis=1, inplace=True)
        self.features.remove(code_feats[0])
        columns = train_df.columns
        index = train_df.index
        if imputer.strategy == 'median' and self.imputing_hack:
            # Hack - for some reason this drops the first column
            train_df.insert(0, 'tmp', 0)
        X_train = pd.DataFrame(imputer.fit_transform(train_df))
        X_train.columns = columns
        X_train[scale_cols] = scaler.fit_transform(X_train[scale_cols])
        X_train = X_train[self.features]
        X_train.index = index

        test_df = test_df[test_df[code_feats].sum(axis=1) == 1]
        test_df.drop(code_feats[0], axis=1, inplace=True)
        index = test_df.index
        if imputer.strategy == 'median' and self.imputing_hack:
            # Hack
            test_df.insert(0, 'tmp', 0)
        # Use transform instead of fit_transform to impute and scale with
        # the same values as in train
        X_test = pd.DataFrame(imputer.transform(test_df))
        X_test.columns = columns
        X_test[scale_cols] = scaler.transform(X_test[scale_cols])
        X_test = X_test[self.features]
        X_test.index = index

        y_train = train_df[self.label]
        y_test = test_df[self.label]

        if imputer.strategy == 'median' and self.imputing_hack:
            # Hack
            train_df.drop('tmp', axis=1, inplace=True)
            test_df.drop('tmp', axis=1, inplace=True)

        try:
            self.clf.set_params(**self.p)
            y_pred_probs_test = self.clf.fit(X_train,
                                        y_train).predict_proba(X_test)[:, 1]
            y_pred_probs_train = self.clf.predict_proba(X_train)[:,1]
            print self.clf
            ks = [0.05, 0.3, 0.6, 0.9]
            results = {'model': self.clf,
                       'metrics': [(k,run_models.metrics_at_k(y_test,
                                                              y_pred_probs_test, k, self.inc_weights[self.bucket_name])) for k in ks],
                       'features': list(X_train.columns),
                       'timestamp': datetime.datetime.now()}
        except IndexError, e:
            print 'Error:', e

        try:
            feat_imps = self.clf.feature_importances_
        except AttributeError:
            try:
                feat_imps = self.clf.coef_[0]
            except AttributeError:
                feat_imps = [0 for _ in results['features']]

        file_name_json = self.storage_path_json + self.hash_name() + '.jsonb'
        tgt = open(file_name_json, 'w')
        json.dump({'LABEL': self.label, 
                   'MODEL SPEC': str(results['model']),
                   'MODEL METRICS':results['metrics'],       
                   'MODEL FEATS & IMPS': zip(results['features'], feat_imps),
                   'MODEL TIME': str(results['timestamp']),
                   'BUCKET NAME': self.bucket_name,        
                   'BUCKET CODES': self.bucket_codes,
                   'HASH': self.hash_name(),
                   'WEIGHTS': self.inc_weights[self.bucket_name]}, 
                  indent=4, separators=(',',':'), fp=tgt)

        tgt.close()
        
        incident_ids_train = y_train.index
        df_train = pd.DataFrame(columns=[self.key, 'score'])
        df_train[self.key] = incident_ids_train
        df_train['score'] = y_pred_probs_train

        incident_ids_test = y_test.index
        df_test = pd.DataFrame(columns=[self.key, 'score'])
        df_test[self.key] = incident_ids_test
        df_test['score'] = y_pred_probs_test

        file_name_pkl = self.storage_path_pkl + self.hash_name() + '.p'
        pickle.dump({'test':df_test, 'train':df_train}, open(file_name_pkl, "wb"))
        lookup_tbl['TIMESTAMP'] = [results['timestamp']]
        lookup_tbl['ID'] = [results['model']]
        lookup_tbl['JSON'] = [open(file_name_json, 'r').read()]
        lookup_tbl['PICKLE'] = [file_name_pkl]
        
        self.pgw.df_to_pg(lookup_tbl, self.tbl_all_info,
                          self.schema_model, if_exists='append')
        self.pgw.create_timestamp(self.success_file % self.hash_name())
