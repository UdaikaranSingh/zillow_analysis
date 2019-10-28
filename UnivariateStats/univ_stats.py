#!/usr/bin/env python

import pandas as pd
import sys
import os


class SingleTableReader(object):
    
    def __init__(self, db, table, basepath='./'):
        self.db = db
        self.table = table
        self.layout = os.path.join(basepath, 'Layout.xlsx')
        self.basepath = basepath
        self._get_schema()
    
    def _sql_to_pandas_dtypes(self, row):
        ct, ml = row['DateType'], row['MaxLength']

        if ct in ['char', 'varchar', 'date', 'time']:
            if (ml <= 50) and (ct not in ['date', 'time']):
                return 'category'
            else:
                return 'str'
        elif ct in ['money', 'decimal']:
            return 'float64'
        elif ct in ['int', 'bigint']:
            return 'Int64'
        else:
            raise Exception('Unseen type')

    def _get_schema(self):

        # select table in excel sheet
        sch = pd.read_excel(self.layout, sheet_name=self.db)
        table = 'ut' + self.table.split('.')[0]
        table_sch = sch.query('TableName == "%s"' % table)
        self.tabsch = table_sch
        # column names
        self.columns = table_sch['FieldName'].values.tolist()
        
        # translate SQL types to Pandas types
        dtype = (
            table_sch
            .assign(pandas_type=table_sch.apply(self._sql_to_pandas_dtypes, axis=1))
            .set_index('FieldName')
            .pandas_type
            .to_dict()
        )
        self.dtype = dtype

        # get date columns for reading into pandas
        date_cols = table_sch.loc[table_sch.DateType.isin(['date', 'time'])]['FieldName'].values.tolist()
        self.date_cols = date_cols
            
    def read(self, **kwargs):

        date_parser = lambda x: pd.to_datetime(x, format='%Y-%m-%d', errors='coerce')
        if 'usecols' in kwargs:
            self.date_cols = list(set(kwargs['usecols']) & set(self.date_cols))

        read_csv_kwargs = dict(
            names=self.columns, 
            sep='|', 
            parse_dates=self.date_cols, 
            dtype=self.dtype,
            date_parser=date_parser,
            quoting=3,
            skipinitialspace=True,
            encoding='windows-1252'
        )

        read_csv_kwargs.update(kwargs)
        
        dg = pd.read_csv(os.path.join(self.basepath, self.db, '%s.txt' % self.table), 
                         **read_csv_kwargs)
        return dg


def basic_stats(ser):
    
    out_stat = {}
    out_stat['col'] = ser.name
    out_stat['tot'] = ser.shape[0]
    out_stat['dtype'] = str(ser.dtype)
    out_stat['nunique'] = ser.nunique()
    out_stat['numnull'] = ser.isnull().sum()
    
    if (ser.dtype.name in ['object', 'category', 'Int64']) and (out_stat['nunique'] < 10000):
        vc = ser.value_counts(dropna=False, normalize=True)
        out_stat['stats'] = vc
        
    elif ser.dtype.name == 'datetime64[ns]':
        vc = ser.value_counts(dropna=False)
        out_stat['stats'] = vc
        
    elif ser.dtype.name == 'float64':
        descr = ser.describe()
        out_stat['stats'] = descr

    else:
        out_stat['stats'] = pd.Series(name=ser.name)
        
    return out_stat


def main(n, basepath):

    outbase = os.path.join(basepath, 'univariate_stats')
    os.makedirs(outbase, exist_ok=True)

    reader = SingleTableReader('ZTrans', 'Main', basepath=basepath)
    col = reader.columns[n]

    fp = os.path.join(outbase, 'pop_%s.csv' % col)
    if os.path.exists(fp):
        return

    ser = reader.read(usecols=[col], squeeze=True)
    out_stat = basic_stats(ser)

    fp = os.path.join(outbase, 'vc_%s.csv' % col)
    out_stat['stats'].to_csv(fp, header=True)
    del out_stat['stats']

    fp = os.path.join(outbase, 'pop_%s.csv' % col)
    pd.Series(out_stat).to_csv(fp, header=False)

    return


if __name__ == '__main__':

    basepath, colnum = sys.argv[1:]

    main(int(colnum), basepath)
