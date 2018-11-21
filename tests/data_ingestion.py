import requests
import pandas as pd

r = requests.get('https://coinmetrics.io/api/v1/get_supported_assets')  

assets = r.json()

def get_asset_data_types(asset): # -> List<Str>
    """Returns a list of columns for a particular given asset"""
    return requests.get('https://coinmetrics.io/api/v1/get_available_data_types_for_asset/{}'.format(asset)).json()['result']

asset_data_types_dict = {asset:get_asset_data_types(asset) for asset in assets}
"""
Dict(Str -> Dict(...))
or
Dict(Str -> List<Str>)
"""
#import datetime
from datetime import datetime
#from datetime import timezone
import time

def date_to_unix(d): # Str -> Int
    """
    Input
    d : Str
        Date in String e.g. '2018-11-19' (Year,Month,Day)
    
    Output
    unix timestamp in string
    """
    dd = datetime.strptime(d, '%Y-%m-%d')
    # Convert datetime into unix timestamp in seconds
    return int(time.mktime(dd.timetuple()))

def unix_to_date(un): # Int -> datetime.datetime
    return datetime.utcfromtimestamp(un).strftime('%Y-%m-%d')

date_today = date_to_unix(datetime.today().strftime('%Y-%m-%d'))

def get_asset_data_for_time_range(asset, col_name, begin_timestamp, end_timestamp):# () -> List<List<Int,Float>>
    req = requests.get("https://coinmetrics.io/api/v1/get_asset_data_for_time_range/{}/{}/{}/{}".\
                       format(asset,col_name,begin_timestamp,end_timestamp))
    req.json()
    return req.json()['result']
#print(get_asset_data_for_time_range('ltc','medianfee',date_to_unix('2010-01-01'),date_today))

#ll = get_asset_data_for_time_range('ltc','medianfee',date_to_unix('2010-01-01'),date_today)
#print( {l[0]:l[1] for l in ll} )
#print( pd.Series({l[0]:l[1] for l in ll}) )
#del ll

def get_dataframe(asset):
    """Returns a DataFrame Containing the column names and the historical data"""
    cols = asset_data_types_dict[asset] # List of Column Names
    frame_dict = dict()
    # For each available column in asset
    for col in cols:
        # Create a Dict(Column Name -> pandas.Series)
        ll = get_asset_data_for_time_range(asset,col,date_to_unix('2010-01-01'),date_today)
        frame_dict[col] = pd.Series({datetime.strptime(unix_to_date(l[0]), '%Y-%m-%d'):l[1] for l in ll})
    return pd.DataFrame(frame_dict)

#datetime.strptime(unix_to_date(1542412800), '%Y-%m-%d') to convert Unix (Int) -> Datetime

for asset in assets:
    # Export csv in .\data\RawData
    get_dataframe(asset).to_csv("..\data\RawData\{}.csv".format(asset))









