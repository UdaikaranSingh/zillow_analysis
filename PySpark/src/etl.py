
import os

import pyspark.sql.functions as F

from pyspark.sql import Window

from schema import create_schema


# ---------------------------------------------------------------------
# File IO
# ---------------------------------------------------------------------

def read_zillow(sqlc, basepath, database, table, **kwargs):
    '''
    read in zillow table as spark dataframes. Assumes that the
    `basepath` directory contains the Layout.xlsx file, as well
    as the databases (ZTrans and ZAsmt directories).
    '''

    layout = os.path.join(basepath, 'Layout.xlsx')
    filepath = os.path.join(basepath, database, table + '.txt')

    schema = create_schema(layout, database=database, table=table)

    df = sqlc.read.csv(
        filepath,
        sep='|', 
        ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True,
        schema=schema,
        encoding='windows-1252'
    )

    return df

# ---------------------------------------------------------------------
# Column Creation (e.g. Date Logic, other flags)
# ---------------------------------------------------------------------

def set_date(df):
    '''Sets a `DATE` column using logic'''

    primary_col, secondary_col = 'DocumentDate', 'RecordingDate'

    out = df.withColumn(
        'DATE',
        F.when(
            F.isnull(F.col(primary_col)), F.col(secondary_col)
        ).otherwise(F.col(primary_col))
    )

    return out


# ---------------------------------------------------------------------
# Filtering logic
# ---------------------------------------------------------------------

def filter_by_price(df):
    '''filters out rows with SalesPriceAmount small'''
    
    thresh = 400
    out = df.filter(F.col('SalesPriceAmount') >= thresh)

    return out


def filter_by_doctype(df):
    '''filters transactions from a dataframe with
    DocumentTypeStndCode, keeping only the N most common
    codes that make up >= 95% of the transactions.

    Warning: this function runs an 'action' on the dataframe,
    and may take a while to run.
    '''

    # build a table of counts from df (executes an action
    # on the dataframe df).
    docs = (
        df
        .select('DocumentTypeStndCode')
        .groupBy('DocumentTypeStndCode')
        .count()
        .toPandas()
    )

    # Find the "top N doc types"
    doc_types = (
        docs
        .set_index('DocumentTypeStndCode')
        .apply(lambda x: x / x.sum())
        .squeeze()
        .sort_values(ascending=True)
        .cumsum()
        .loc[lambda x:x >= 0.05]
        .index
        .tolist()
    )

    out = df.filter(F.col('DocumentTypeStndCode').isin(doc_types))

    return out


# ---------------------------------------------------------------------
# Sequential/Property based-filtering (e.g. repeated sales)
# ---------------------------------------------------------------------

def drop_repeated(df):
    '''adds a `TransIdRepeated` column that considers Transaction occurring
    within a 90 day chain of transactions to be part of the same transaction.
    df must have ImportParcelID and DATE columns.'''

    TIME_THRESH = 90
    wind = Window.partitionBy("ImportParcelID").orderBy("DATE")
    out = (
        df
        .withColumn(
            'time_between', 
            F.datediff(F.col('DATE'), F.lag(F.col('DATE'), 1).over(wind))
        )
        .fillna(0, subset='time_between')
        .withColumn(
            'change', 
            F.when(F.col('time_between') < TIME_THRESH, 0).otherwise(1)
        )
        .withColumn(
            'TransIdRepeated', 
            F.sum(F.col('change')).over(wind)
        )
        .drop('change', 'time_between')
    )

    return out

# ---------------------------------------------------------------------
# Table Joining Logic
# ---------------------------------------------------------------------

def join_tables(dflist, how='outer'):
    '''
    join a list of dataframes on the maximal
    subset of pairwise common columns (similar to
    the logic in Pandas `merge` method).
    '''

    tab = dflist[0]
    for df in dflist[1:]:
        cols = list(set(tab.columns) & set(df.columns))
        tab = tab.join(df, on=cols, how=how)

    return tab
