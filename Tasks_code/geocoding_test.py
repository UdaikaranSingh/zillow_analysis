import numpy as np
import pandas as pd
from arcgis.gis import GIS
from arcgis.geocoding import geocode, batch_geocode
from ZillowUnivariate.univ_stats import SingleTableReader

gis = GIS(username='u1singh_UCSDOnline9', password='Udaikaran98')

file_dir = '/Volumes/GoogleDrive/Shared drives/Zillow1/data/raw/ZTrans_ZAsmt/unzipped_20191001/02'

columnsToUse = ['PropertyFullStreetAddress', 
                'PropertyCity','PropertyState','PropertyZip']

prop_table = SingleTableReader("ZTrans", "PropertyInfo", file_dir).read(nrows = 5000, usecols = columnsToUse)

def define_address(row):
    state = row['PropertyState']
    zipCode = row['PropertyZip']
    city = row['PropertyCity']
    address = row['PropertyFullStreetAddress']
    if not pd.isna(state):
        if not pd.isna(zipCode):
            if not pd.isna(city):
                if not pd.isna(address):
                    return address + ',' + city + ',' + state + ',' + zipCode
                else:
                    return city + ',' + state + ',' + zipCode
            else:
                return state + ',' + zipCode
        else:
            return state
    else:
        return np.nan

prop_table['fixed_address'] = prop_table.apply(define_address, axis = 1)

addresses = list(prop_table.fixed_address)
results = batch_geocode(addresses)


df = pd.DataFrame(columns = ['score', 'latitude', 'longitude', 'address'])
count = 0
for res in results:
    if (res == None):
        row = [np.nan, np.nan, np.nan, np.nan]
    elif (res['score'] == 0):
        row = [np.nan, np.nan, np.nan, np.nan]
    else:
        row = [res['score'], res['attributes']['X'],
               res['attributes']['Y'], res['attributes']['Place_addr']]
    df.loc[len(df)] = row

fin_table = pd.concat([prop_table,df], axis = 1)
fin_table.to_csv("sample_geocoding.csv")