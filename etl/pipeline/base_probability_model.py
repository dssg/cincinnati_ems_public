##Imports
import pandas as pd
import operator
import psycopg2
import pylab
import numpy as np
import datetime
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from collections import Counter
import matplotlib.patches as mpatches
from scipy.stats.stats import pearsonr
import matplotlib.lines as mlines
import matplotlib as mpl
from matplotlib import cm
from mpl_toolkits.axes_grid1 import make_axes_locatable
from statsmodels.tsa import stattools
import statsmodels.api as sm
import scipy
import random
import seaborn as sns
from matplotlib.font_manager import FontProperties
import matplotlib.mlab as mlab
import re
from collections import OrderedDict
import statsmodels.api as sm
from scipy import stats
import statsmodels
from statsmodels.graphics.api import qqplot
from luigi import configuration
from sklearn import linear_model, datasets

mpl.rcdefaults()
pd.options.display.mpl_style = 'default'

##Read database parameters from default_profile

host = configuration.get_config().get('postgres', 'host')
database = configuration.get_config().get('postgres', 'database')
user = configuration.get_config().get('postgres', 'user')
password  = configuration.get_config().get('postgres', 'password')

conn_sqlalch = create_engine('postgresql://{}:{}@{}/{}'.format(user, password, host, database))

try:
    conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'"%(database,user,host,password))
except:
    print "Unable to connect to the database"
    
##Functions

def is_NLN_format(s):
    """
    input: string
    output: True if s is in Number-Letter-Number format and False otherwise
    """
    hit = re.match(r'\d{1,2}[A-Z](\d{1,2}|O)', s, flags = 0)
    if hit:
        return True
    return False

def prob_trns(typ, sev=''):
    """
    Inputs typ which is numeric code of incident
    and sev which is severity level: A,B,C,D,E...
    
    Return Double (x,y)
    x = probability that incident requires tranport
    y = probability that a given incident has the given type
    
    If no such incident, return error
    """
    inc_type = typ+sev
    try:
        return trns_acc[inc_type]
    except:
        return 'Undefined Incident Type'

def gen_transport_prob_dict(full_df):
"""
input: features dataframe
output: baseline probabilities
"""

    master_dict = {\
    1:'ABDOMINAL PAIN',\
    2:'ALLERGIES',\
    3:'ANIMAL BITES',\
    4:'ASSAULT',\
    5:'BACK PAIN',\
    6:'BREATHING',\
    7:'BURNS',\
    8:'CARBON MONOXIDE',\
    9:'CARIAC/RESP ARREST',\
    10:'CHEST PAIN',\
    11:'CHOKING',\
    12:'CONVULSIONS/SEIZURES',\
    13:'DIABETIC PROBS',\
    14:'DROWNING',\
    15:'ELECTROCUTION',\
    16:'EYE PROBS',\
    17:'FALLS',\
    18:'HEADACHE',\
    19:'HEART PROBLEMS',\
    20:'HEAT/COLD EXPOSURE',\
    21:'HEMORRHAGE/LACERATIONS',\
    22:'ENTRAPMENT',\
    23:'OVERDOSE/POISONING',\
    24:'PREGNANCY',\
    25:'PHYCHIATRIC/SUICIDE ATTEMPT',\
    26:'SICK PERSON',\
    27:'STAB/GUNSHOT WOUND',\
    28:'STROKE',\
    29:'TRAFIC/TRANPORTATION INC',\
    30:'TRAUMATIC INJURIES',\
    31:'UNCONSCIOUS/FAINTING',\
    32:'UNKNOWN'}

    #add tag for if a type id is in Number-Letter-Number format
    full_df['is_NLN'] = full_df['iti_typeid'].apply(is_NLN_format)

    #group by type
    gb_type = full_df.groupby('iti_typeid')

    #initialize transport accuracy dictionary
    trns_acc = {}
    #populate dictionary
    for name,group in gb_type:
        trns_acc[name] = ((group['trns_to_hosp']>0).sum()/float(len(group)), len(group)/float(len(full_df)))
    
    #return dictionary of transport accuracy
    return trns_acc

def gen_logistic_results(train_df, test_df):

    """
    input: train and test tables
    output: dataframe of logistic regression results"
    """
    
    #add features to training dataframe
    train_df['hour'] = train_df['i_ttimecreate'].apply(lambda x: x.hour)
    train_df = train_df[train_df['trns_to_hosp'].apply(lambda x: type(x) == bool)]
    train_df['type'] = ['train']*len(train_df)

    #add features to testing dataset
    test_df['hour'] = test_df['i_ttimecreate'].apply(lambda x: x.hour)
    test_df = test_df[test_df['trns_to_hosp'].apply(lambda x: type(x) == bool)]
    test_df['type'] = ['test']*len(test_df)

    #temporarily join the train and test sets
    full_df = train_df.append(test_df)
    thresh = 50
    base = {}
    scores = {}
    gb_type = full_df.groupby('iti_typeid')

   
    for name,group in gb_type:
        #keep only event types with more than thresh values
        if len(group) < thresh:
            continue

        train_set = group[group['type'] == 'train']
        test_set = group[group['type'] == 'test']

        #create training set and testing set for regressors
        vars = ['is_4', 'is_5', 'is_6', 'is_7', 'is_8', 'is_9',/
                'is_10', 'is_11', 'is_12', 'is_13', 'is_14', 'is_15', /
                'is_16', 'is_17, is_18', 'is_19', 'is_20', 'is_21', / 
                'is_22', 'is_23', 'is_24', 'is_1', 'is_2',]
        
        X_train = train_set['hour'][:, np.newaxis]
        X_test = test_set['hour'][:, np.newaxis]
    
        #create training set and testing set for labels
        Y_train = train_set['trns_to_hosp']
        Y_test = test_set['trns_to_hosp']
        
        #we can only use logistic regression if there are 0s and 1s in the training labels
        if len(set(Y_train)) > 1:
            logreg = linear_model.LogisticRegression()
            logreg.fit(X_train,Y_train)
            pred = logreg.predict(X_test)
            #add accuracy to scores dictionary
            scores[name] = sum([int(i==j) for i,j in zip(pred, Y_test)])/float(len(Y_test))
            
            #add base probability scores to base dictionary
            prob = sum(Y_train)/float(len(Y_train))
            base[name] = prob**2 + (1-prob)**2
            
    #convert dictionary of scores to dataframe
    results = pd.DataFrame(scores.items(), columns = ['inc_type_id', 'logistic_acc'])
    
    #add base accuracy to table only for which we have a logistic accuracy
    base_list = []
    for item in scores.keys():
        base_list.append(base[item])
    results['base_acc'] = base_list
    results['pct_acc_gain'] = (results['logistic_acc'] - results['base_acc'])/results['base_acc']
    return results
            
