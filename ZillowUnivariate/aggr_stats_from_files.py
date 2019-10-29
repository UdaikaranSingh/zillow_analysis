#!/usr/bin/env python
# coding: utf-8

# In[ ]:


get_ipython().run_line_magic('matplotlib', 'inline')

import pandas as pd
import numpy as np
import os
import json
import sys

pd.options.display.float_format = '{:,.3f}'.format


# In[ ]:





# In[ ]:


base = 'univariate_stats/'
vc = {}
for b, d, f in os.walk(base):
    for file in f:
        if file[:3] == 'vc_':
            fips = b[len(base):]
            c = file[3:-4]
            vals = pd.read_csv(os.path.join(b, file), index_col=0, squeeze=True).to_dict().items()
            vc.update({(fips, c, k): v for (k,v) in vals})


# In[ ]:


ser = pd.Series(vc)


# In[ ]:


ser.to_frame().info()


# In[ ]:


vc_date = ser.loc[ser.index.get_level_values(1).str.contains('Date')]

statsvals = ['count', 'max', 'min', 'mean', 'std', '25%', '50%', '75%']
vc_nums = ser.loc[ser.index.get_level_values(2).isin(statsvals)]

vc_cats = ser.loc[~(ser.index.get_level_values(1).str.contains('Date') | ser.index.get_level_values(2).isin(statsvals))]


# In[ ]:





# In[ ]:


num_col_cnts = (
    vc_nums
    .reset_index()
    .rename(columns={'level_0': 'State', 'level_1': 'Column', 'level_2': 'Value'})
    .groupby('Column')
    .apply(lambda x:x.pivot_table(columns='State', index='Value', values=0))
)


# In[ ]:


num_col_cnts.to_csv('univariate_numeric_cols.csv')


# In[431]:


num_col_cnts


# In[ ]:


date_col_cnts = (
    vc_date
    .reset_index()
    .rename(columns={'level_0': 'State', 'level_1': 'Column', 'level_2': 'Value'})
    .fillna({'Value':'nan'})
    .groupby('Column')
    .apply(lambda x:x.pivot_table(columns='State', index='Value', values=0))
    .fillna(0)
)


# In[ ]:


date_col_cnts.head()


# In[ ]:


date_col_cnts.to_csv('univariate_date_cols.csv')


# In[ ]:


cols = np.unique(date_col_cnts.index.get_level_values(0))

with pd.ExcelWriter('univariate_date_cols.xlsx') as writer:
    for c in cols:
        a = date_col_cnts.loc[date_col_cnts.index.get_level_values(0) == c].reset_index(level=0, drop=True)
        a.to_excel(writer, sheet_name=c)


# In[ ]:





# In[ ]:





# In[ ]:


cat_col_cnts = (
    vc_cats
    .reset_index()
    .rename(columns={'level_0': 'State', 'level_1': 'Column', 'level_2': 'Value'})
    .fillna({'Value':'nan'})
)


# In[ ]:


dflist = []
G = cat_col_cnts.groupby('Column')
for col in G.groups.keys():
    try:
        a = G.get_group(col).pivot_table(columns='State', index='Value', values=0, fill_value=0)
        a.index = pd.MultiIndex.from_tuples([[col, x] for x in a.index], names=['Column', 'Value'])
        dflist.append(a)
    except:
        print(col)
        raise


# In[ ]:


cat_col_cnts = pd.concat(dflist, sort=False).T.sort_index().T.fillna(0)


# In[ ]:


cat_col_cnts.to_csv('univariate_categorical_cols.csv')


# In[361]:


cols = np.unique(cat_col_cnts.index.get_level_values(0))

with pd.ExcelWriter('univariate_categorical_cols.xlsx') as writer:
    for c in cols:
        a = (
            vc_cats[vc_cats.index.get_level_values(1) == c]
            .reset_index(level=1, drop=True)
            .reset_index()
            .fillna({'level_1': 'nan'})
            .rename(columns={'level_0': 'State', 'level_1': 'Value'})
            .pivot_table(columns='State', index='Value', values=0)
        )

        a = a.assign(m=a.fillna(0).mean(axis=1)).sort_values('m', ascending=False).fillna(0).iloc[:25]

        a.to_excel(writer, sheet_name=c)


# In[429]:


cat_col_cnts.head()


# In[419]:


base = 'univariate_stats/'
pops = []
for b, d, f in os.walk(base):
    for file in f:
        if file[:4] == 'pop_':
            fips = b[len(base):]
            vals = pd.read_csv(os.path.join(b, file), index_col=0, squeeze=True, header=None)
            vals['fips'] = fips
            pops.append(vals)


# In[417]:





# In[ ]:





# In[426]:


a = pd.DataFrame(pops)
a['tot'] = a['tot'].astype(int)
a['unique'] = a['nunique'].astype(int)
a['numnull'] = a['numnull'].astype(int)
a['pct_null'] = a['numnull'] / a['tot']
a = a.set_index(['fips', 'col'], drop=True).sort_index()


# In[428]:


a.to_csv('univariate_population_stats.csv')


# In[ ]:





# In[ ]:




