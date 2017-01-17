# coding utf-8
# Luigi Imports
import luigi
import luigi.postgres
from luigi import configuration, LocalTarget
from sklearn.preprocessing import Imputer, StandardScaler
# External Imports
import run_reg_models
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

from etl.pipeline.feature_creation_demand import CreateDemandFeatureTable

# Generate Sample Data
class SampleDataReg(PostgresTask):

    # Parameter values are found in luigi.cfg
    days_to_predict = luigi.Parameter()
    initial_train_year = luigi.Parameter()

    schema_feats = luigi.Parameter()
    train_data_feats = luigi.Parameter()

    schema_model = luigi.Parameter()
    tbl_train = luigi.Parameter()
    tbl_test = luigi.Parameter()
    feat_types = luigi.Parameter()
    
    model_specs_file = luigi.Parameter()
    success_file = luigi.Parameter()

    label = luigi.Parameter()
    label_lags = luigi.Parameter()

    def requires(self):
        # We need the semantic table to exist and the model schema to exist
        return [CreateDemandFeatureTable(), CreateSchema(self.schema_model)]

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

        dict_func = {}
        for col in feature_df.columns:
            if col in ['m_required', 'trns_to_hosp']:
                dict_func[col] = np.sum
            elif col not in ['station_name', '_date', 'pk_demand', 'time_of_day']:
                dict_func[col] = np.mean

        day_df = feature_df.groupby(['_date', 'station_name']).agg(dict_func)

        day_df.reset_index(inplace = True)

        day_df['time'] = day_df.apply(lambda x: datetime.datetime(int(x.time_year), int(x.time_month), int(x.time_day)), axis = 1)

        day_df.drop(['time_year', 'time_month', 'time_day'], axis = 1, inplace = True)

        day_df.drop('_date', axis=1, inplace=True)

        feature_df = day_df

        feature_df['demand_key'] = feature_df.apply(lambda x: str(x.time) + x.station_name, axis =1)

        ind_zip = zip(feature_df.time, feature_df.station_name)

        index_ts = pd.MultiIndex.from_tuples(ind_zip, names=['day', 'station'])

        feature_df.index = index_ts
        
        for lag in [int(i) for i in self.label_lags.split()]: 
            l = []
            for i,row in feature_df.iterrows():
                try:
                    l.append(feature_df.ix[i[0]-datetime.timedelta(lag)].ix[i[1]][self.label])
                except KeyError:
                    l.append(0)
            feature_df[self.label + '_lag' + str(lag)] = l

        feature_df.reset_index(inplace=True)
        

        # categorical variable binary generation
        categorize = []
        for i, row in data_types_df.iterrows():
            if row['type'] == 'cat' and row['name'] != 'station_name':
                categorize.append(row['name'])

        # drop one of the binary columns to avoid multicolinearity
        for col in [i for i in categorize if i in feature_df.columns]:
            dummy_col = pd.get_dummies(feature_df[col], prefix=col + '_is')
            feature_df = feature_df.join(dummy_col)
            tot = [i for i in feature_df.columns if col + '_is' in i]
            feature_df = feature_df.drop(tot[0], 1)

        # Load train table into database

        start_time = day_df.time.max() - datetime.timedelta(int(self.days_to_predict))
        train_df = feature_df[(feature_df['time'].apply(lambda x: x.year >= int(self.initial_train_year))) & (feature_df['time'] < start_time)]

        test_df = feature_df[feature_df['time'] >= start_time]

        # drop the original categorical column
        for col in [i for i in categorize if i in feature_df.columns]:
            train_df = train_df.drop(col, 1)
            test_df = test_df.drop(col, 1)

        # Load test table into database
        pk = 'demand_key'
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

# Calculate Demand
class RunModelsReg(PostgresTask):

    # Get luigi parameters from luigi.cfg file
    models_used = luigi.Parameter()
    features = luigi.Parameter()
    success_file = luigi.Parameter()
    model_specs_file = luigi.Parameter()
    
    label = luigi.Parameter()
    
    final_weight = luigi.Parameter()
    schedule = luigi.Parameter()
    overunder = luigi.Parameter()

    stations = luigi.Parameter(default = '02 03 05 07 08 09 12 14 17 18 19 20 21 23 24 29 31 32 34 35 37 38 46 49 50 51')

    def requires(self):
        # get models to use
        models_to_run = [x.strip() for x in self.models_used.split('-')[1:]]
        # Construct union of all codes for computing it's compliment for 
        # 'other' in RunML
        
        clfs, grid = run_reg_models.define_clfs_params()

        final_weight = float(self.final_weight)
        overunder = [float(i) for i in self.overunder.split()]

        station_names = []
        for item in self.stations.split():
            station_names.append('STA' + "%02d" % int(item))

        # Wrapper loop for running all models
        runs = []
        counter = 0
        for index, clf in enumerate([clfs[x] for x in models_to_run]):
            parameter_values = grid[models_to_run[index]]
            for p in ParameterGrid(parameter_values):
                for station in station_names:
                    runs.append(RunMLReg(p, clf, features=self.features,
                                station = station, 
                                final_weight=final_weight,
                                schedule=self.schedule,
                                overunder=overunder,
                                  model_counter=counter,
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


class RunMLReg(PostgresTask):
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
    
    schedule = luigi.Parameter()
    final_weight = luigi.Parameter()
    overunder = luigi.Parameter()

    station = luigi.Parameter()
    

    def __init__(self, p, clf, *args, **kwargs):
        super(RunMLReg, self).__init__(*args, **kwargs)

        self.clf = clf
        self.p = p
        columns = self.pgw.get_column_names(
            table_name=self.tbl_train, schema=self.schema_model)
        # get features
        feature_classes = [i for i in self.features.replace(
            '\n', '').split('- ') if i != '']

        # Code bucketing
        features = set([i for e in feature_classes for i in columns if e in i])
        self.feat_str = (', '.join(features) + ', ' + self.label
                         + ', ' + self.key)
        self.features = [x.strip().replace('"', '') for x in features]
        if len([i for i in self.features if 'lag' not in i]) == 0:
            self.baseline = "YES"
        else:
            self.baseline = "NO"
       

    def requires(self):
        return SampleDataReg()

    def output(self):
        return FileOlderTarget(old_file=self.model_specs_file,
                young_file=self.success_file % self.hash_name())

    def hash_name(self):
        # Feat_str has features, key, and label
        # Code bucket implicitly in features
        return hashlib.md5("".join(self.feat_str)
                           + str(self.clf.__class__)
                           + str(self.p)
                           + str(self.label)
                           + str(self.station)
                           + str(self.baseline)
                           ).hexdigest()

    def run(self):
        # we need for the train and test data to exist
        # load train and test tables
        # change * to features to be used
        conn = self.pgw.get_conn()
        train_df = pd.read_sql_query("select {} from {}.{} where station_name = '{}'".format(
            self.feat_str, self.schema_model, self.tbl_train, self.station),
            con=conn)
        train_df.set_index(self.key, inplace=True)
        test_df = pd.read_sql_query("select {} from {}.{} where station_name = '{}'".format(
            self.feat_str, self.schema_model, self.tbl_test, self.station),
            con=conn)
        conn.close()
        test_df.set_index(self.key, inplace=True)

        lookup_tbl = pd.DataFrame(columns=['TIMESTAMP', 'ID', 'JSON', 'PICKLE'])
        
        scaler = StandardScaler()
        imputer = Imputer(strategy='median')
        
        columns = train_df.columns
        index = train_df.index
        if imputer.strategy == 'median':
            # Hack - for some reason this drops the first column
            train_df.insert(0, 'tmp', 0)
            pass
        X_train = pd.DataFrame(scaler.fit_transform(
                               imputer.fit_transform(train_df)))

        X_train.columns = columns
        X_train = X_train[self.features]
        X_train.index = index

        index = test_df.index
        if imputer.strategy == 'median':
            # Hack
            test_df.insert(0, 'tmp', 0)
            pass
        # Use transform instead of fit_transform to impute and scale with
        # the same values as in train
        X_test = pd.DataFrame(scaler.transform(imputer.transform(test_df)))
        
        
        X_test.columns = columns
        X_test = X_test[self.features]
        X_test.index = index

        y_train = train_df[self.label]
        y_test = test_df[self.label]

        if imputer.strategy == 'median':
            # Hack
            #train_df.drop('tmp', axis=1, inplace=True)
            #test_df.drop('tmp', axis=1, inplace=True)
            pass

        try:
            self.clf.set_params(**self.p)
            y_pred = self.clf.fit(X_train,
                                  y_train).predict(X_test)
            print self.clf
           
            results = {'model': self.clf,
                       'metrics': run_reg_models.reg_metrics(y_test,
                                                             y_pred, self.final_weight, self.schedule, self.overunder),
                       'features': list(X_train.columns),
                       'timestamp': datetime.datetime.now()}
        except IndexError, e:
            print 'Error:', e

        try:
            feat_imps = self.clf.coef_
        except AttributeError:
            feat_imps = ['NULL']*len(columns)

        file_name_json = self.storage_path_json + self.hash_name() + '.jsonb'
        tgt = open(file_name_json, 'w')
        json.dump({'LABEL': self.label,
                   'STATION': self.station,
                   'MODEL SPEC': str(results['model']),
                   'MODEL METRICS':results['metrics'],       
                   'MODEL FEATS & IMPS': zip(results['features'], feat_imps),
                   'MODEL TIME': str(results['timestamp']),
                   'BASELINE': self.baseline
                  }, 
                  indent=4, separators=(',',':'), fp=tgt)

        tgt.close()
        
        incident_ids = y_test.index
        df = pd.DataFrame(columns=[self.key, 'score'])
        df[self.key] = incident_ids
        df['pred'] = y_pred
        file_name_pkl = self.storage_path_pkl + self.hash_name() + '.p'
        pickle.dump(df, open(file_name_pkl, "wb"))
        lookup_tbl['TIMESTAMP'] = [results['timestamp']]
        lookup_tbl['ID'] = [results['model']]
        lookup_tbl['JSON'] = [open(file_name_json, 'r').read()]
        lookup_tbl['PICKLE'] = [file_name_pkl]
        
        
        self.pgw.df_to_pg(lookup_tbl, self.tbl_all_info,
                          self.schema_model, if_exists='append')
        self.pgw.create_timestamp(self.success_file % self.hash_name())
