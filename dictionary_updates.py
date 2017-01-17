
# # Dictionary Updates
# Update dictionary based on code severities, their weighting, and observed fraction of transports

# ## Imports

import pandas as pd
import utils.pg_tools as pg
import luigi, luigi.configuration
import numpy as np

pgw = pg.PGWrangler(dbitems = pg.get_pgdict_from_cfg())
luigi_config = luigi.configuration.get_config()

buckets = {}
for x in luigi_config.get('RunModels','code_buckets').split('-')[1:]:
    (key, value) = x.strip().split(':')
    buckets[key.strip()] = [y.strip() for y in value.strip().split(',') if not y is '']
bucket_bycode = {}
for key in buckets:
    for val in buckets[key]:
        bucket_bycode[val] = key

bucket_weights = {}
for x in luigi_config.get('DictOptimization', 'code_weights').split('- ')[1:]:
    (key, value) = x.strip().split(':')
    bucket_weights[key.strip()] = np.array([y.strip() for y in value.strip().split(' ')]).reshape((2,2)).astype(float)


# ## Weights for each severity
# Read as:
# 
# [[ tp, fn]
#   [ fp, tp]]


print("Weighting per class")
for key in bucket_weights:
    if not key=='other':
        print(key)
        print(bucket_weights[key])
        print(buckets[key])
        print("")


# ## Read data
conn = pgw.get_conn()
train_years = (2013, 2014)
test_years = (2015, 2015)
sql = """
      select dispatch_code, code_type, code_level, code_rest, 
          avg(trns_to_hosp::int) as frac_trans, 
          count(incident) as n_incidents,
          avg(m_required::int) as mt_required 
      from semantic.master
      where time_year >= %d and time_year <= %d
      group by dispatch_code, code_type, code_level, code_rest
      order by count(incident) DESC
      """
train_df = pd.read_sql(sql%train_years, conn)
test_df = pd.read_sql(sql%test_years, conn)
conn.close()
assert(set(train_df.mt_required.unique()) == {0,1} and set(test_df.mt_required.unique()) == {0,1})
train_df.mt_required = train_df.mt_required.astype('bool')
test_df.mt_required = test_df.mt_required.astype('bool')

train_df.set_index('dispatch_code', inplace=True)
test_df.set_index('dispatch_code', inplace=True)
common_codes = set(train_df.index) & set(test_df.index)
train_df = train_df.ix[list(common_codes)]
train_df.sort_values(by='n_incidents', ascending=False, inplace=True)
test_df = test_df.ix[list(common_codes)]
test_df.sort_values(by='n_incidents', ascending=False, inplace=True)


# ## Compute suggested transport
# Suggest whenever the score according to the severity weighting is higher for
# transport vs. no transport for all cases of this code

# Add urgency to bucket
def bucket_f(code):
    try:
        return bucket_bycode[code]
    except:
        return 'other'
train_df.insert(5,'urgency', train_df['code_type'].apply(bucket_f))
test_df.insert(5,'urgency', test_df['code_type'].apply(bucket_f))

def suggest_transport(row):
    if row.urgency == 'high':
        return True
    
    tp_sent = row.frac_trans
    fp_sent = 1-tp_sent
    tn_sent = 0
    fn_sent = 0
    confusion_sent = np.array([[tp_sent, fn_sent],[fp_sent, tn_sent]])

    tp_nots = 0
    fp_nots = 0
    tn_nots = 1-row.frac_trans
    fn_nots = 1-tn_nots
    confusion_nots = np.array([[tp_nots, fn_nots],[fp_nots, tn_nots]])

    urgency = row.urgency
    
    score_sent = (bucket_weights[urgency]*confusion_sent).sum()
    score_nots = (bucket_weights[urgency]*confusion_nots).sum()
    
    return score_sent>score_nots
    
train_df['mt_suggested'] = train_df.apply(suggest_transport, axis=1)
test_df['mt_suggested'] = train_df['mt_suggested']


# ## Most frequent codes
print("\nMost frequent codes:")
print(test_df.head(10))

# ## K-computing - not really needed
key = 'mt_required'
k_df = train_df[['n_incidents', key, 'urgency']]
k_df['total_required'] = k_df[key]*k_df['n_incidents']
urgency_grouped = k_df.groupby('urgency')
print("\nFraction transported for each urgency level according to current dispatch protocol:")
print(urgency_grouped['total_required'].sum()/urgency_grouped['n_incidents'].sum())


# ## Resulting increase in transport runs
N_mt_dispatches_suggested = sum(test_df.mt_suggested*test_df.n_incidents)
N_mt_dispatches = int(sum(test_df.mt_required*test_df.n_incidents))
mt_dispatches_increase = (N_mt_dispatches_suggested-N_mt_dispatches)/float(N_mt_dispatches)

N_transports_suggested = N_mt_dispatches_suggested + sum(~test_df.mt_suggested*test_df.frac_trans*test_df.n_incidents)
N_transports = N_mt_dispatches + sum(~test_df.mt_required*test_df.frac_trans*test_df.n_incidents)
mt_transport_increase = (N_transports_suggested-N_transports)/float(N_transports)

print("\nSuggested inrease in mt dispatches: %.2f"%(mt_dispatches_increase))
print("Resulting increase in transport runs: %.2f"%(mt_transport_increase))


# ## Change in confusion matrix
def confusion_matrix(col):
    tp = sum(test_df.n_incidents*test_df[col]*test_df.frac_trans)
    fn = sum(test_df.n_incidents*(~test_df[col])*test_df.frac_trans)
    fp = sum(test_df.n_incidents*test_df[col]*(1-test_df.frac_trans))
    tn = sum(test_df.n_incidents*(~test_df[col])*(1-test_df.frac_trans))
    matrix = np.array([[tp, fn],[fp, tn]])
    matrix /= matrix.sum()
    return matrix


# ### Old confusion matrix
conf_old = confusion_matrix('mt_required')
print("\nOld confusion matrix:")
print(conf_old)


# ### Confusion matrix based on suggestions
conf_new = confusion_matrix('mt_suggested')
print("New confusion matrix:")
print(conf_new)


# ### Absolute difference:
incidents_year = sum(test_df.n_incidents) #2015
print("Absolute effect for the test set (2015)")
print((conf_new-conf_old)*incidents_year)


# ## All proposed changes
fname = "dispatch_dictionary.xls"
ew = pd.ExcelWriter(fname)
test_df.to_excel(ew, "Full data")
test_df[['mt_suggested']].to_excel(ew, "New dictionary")
test_df[~(test_df['mt_suggested']==test_df['mt_required'])].to_excel(ew, "All changes")
ew.save()
print("\nSaved results to "+fname)
