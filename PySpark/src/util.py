
import pyspark.sql.functions as F

from etl import read_zillow, join_tables



def get_all_buyer_seller_for_property(sqlc, ImportParcelID, basepath='.'):
    '''
    Creates a Pandas dataframe consisting of all transactions corresponding to
    a given ImportParcelID, along with Buyer and Seller information.
    '''
    tables = ['PropertyInfo', 'Main', 'BuyerName', 'SellerName']
    dflist = [read_zillow(sqlc, basepath, 'ZTrans', tab) for tab in tables]

    # filter propertyInfo to only the given ImportParcelID
    dflist[0] = (
        dflist[0]
        .select('ImportParcelID', 'TransId')
        .filter(F.col('ImportParcelID') == ImportParcelID)
    )

    out = join_tables(dflist, how='left').toPandas()

    return out
