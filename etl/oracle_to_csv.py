import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('oracle+cx_oracle://SYSTEM:tiger@localhost/CINCI')

p = engine.execute("SELECT owner, table_name from dba_tables where table_name like '%OPDA%'")
for r in p: print(r)

tables = ['OPDA_CFD_SAFETY_PAD_T_INCIDENT','OPDA_CAD_CFD_TURLEY_CAG_VW']

dfs = [pd.read_sql_query('SELECT * FROM %s'%table,engine) for table in tables]
for (i,df) in enumerate(dfs): df.to_csv('%s.csv'%tables[i],sep='|',index=False)