import pandas as pd

from univ_stats import SingleTableReader

'''
# stats of all columns in chunks
# easier to control memory usage than one-column-at-a-time processing


reader = SingleTableReader('ZTrans', 'Main', '10')

chunks = reader.read(chunksize=100000)

is_null = pd.Series(0, index=reader.columns)

numcols = [x for (x,y) in reader.dtype.items() if y in ['Int64', 'float64']]

col_means = pd.Series(0, index=numcols)
col_mins = pd.Series(0, index=numcols)
col_maxs = pd.Series(0, index=numcols)

vc = {}
large_cardinality_columns = []

for dg in chunks:
    is_null = is_null + dg.isnull().sum()
    
    num = dg.select_dtypes('number')
    col_means = (col_means + num.mean()) / 2
    
    m = num.min()
    col_mins[col_mins > m] = m[col_mins > m]
    
    M = num.max()
    col_maxs[col_maxs < M] = M[col_maxs < M]
    
    cat = dg.select_dtypes(exclude=['number'])
    for colname, col in cat.iteritems():
        if (colname not in vc) and (colname not in large_cardinality_columns):
            vc[colname] = col.value_counts(dropna=False)
        elif (colname not in large_cardinality_columns):
            a = vc[colname]
            b = col.value_counts(dropna=False).rename('')
            vc[colname] = a.to_frame().join(b.to_frame()).sum(axis=1)
            
            if vc[colname].shape[0] >= 10000:
                del vc[colname]
                large_cardinality_columns.append(colname)

tot = dg.index[-1]

popstats = pd.concat([
    pd.Series(tot, index=is_null.index, name='total_rows'),
    is_null.rename('null_rows'),
    (is_null / tot).rename('pct_null'),
    col_means.rename('mean'), 
    col_maxs.rename('max'), 
    col_mins.rename('min')
], axis=1, sort=False)

vcdata = {(c, k): [v, v/tot] for c in sorted(vc) for k, v in vc[c].to_dict().items()}
vcdf = pd.DataFrame(vcdata, index=['count', 'percent']).T
'''
