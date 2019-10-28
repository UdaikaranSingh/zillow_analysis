import os
import pandas as pd

import pyspark.sql.types as T

DB2SPARK_TYPE_MAP = {
    'bigint': 'long',
    'char': 'string',
    'varchar': 'string',
    'date': 'date',
    'money': 'decimal',
    'decimal': 'decimal',
    'int': 'integer',
    'time': 'timestamp',
    'tinyint': 'byte',
    'smallint': 'integer'
}


def create_schema(layout_file, database='ZTrans', table='Main'):
    '''create a spark schema from the layout spreadsheet'''

    # Grab field names and types from layout
    layout = pd.read_excel(layout_file, sheet_name=database)
    schema_table = 'ut%s' % table
    tab = (
        layout.loc[layout['TableName'] == schema_table]
        .set_index('column_id')
        .assign(
            type=lambda x: x['DateType'].replace(DB2SPARK_TYPE_MAP),
            name=lambda x: x['FieldName'],
            metadata=None,
            nullable=True
        )
        .loc[:, ['name', 'type', 'metadata', 'nullable']]
    )

    # contruct the spark schema
    types = {'fields': [x.to_dict() for _, x in tab.iterrows()]}
    schema = T.StructType([]).fromJson(types)
    
    return schema
