import os

import pandas as pd
import numpy as np

assets_dict =\
{filename[:-4]:pd.read_csv(f".\data\RawData\{filename}",parse_dates=['Date'],index_col=["Date"]) for filename in os.listdir(r".\data\RawData")}

def get_df(column_name='price(usd)'):
    temp_dict = {}
    for k,v in assets_dict.items():
        try:
            temp_dict[k] = assets_dict[k][column_name]
        except KeyError: # Exception: Column Name does not exist for the asset, this case return numpy.nan
            temp_dict[k] = np.nan
    return pd.DataFrame(temp_dict)

column_names =\
["txcount",
"txvolume(usd)",
"adjustedtxvolume(usd)",
"paymentcount",
"activeaddresses",
"fees",
"medianfee",
"generatedcoins",
"averagedifficulty",
"mediantxvalue(usd)",
"blocksize",
"blockcount",
"price(usd)",
"marketcap(usd)",
"exchangevolume(usd)"]

columns_df = {col:get_df(column_name=col) for col in column_names}

for k,v in columns_df.items():
    if "(usd)" not in k:
        v.to_csv(".\data\ColumnData\{}.csv".format(k),index_label='Date')
    else:
        v.to_csv(".\data\ColumnData\{}.csv".format(k.replace("(usd)","")),index_label='Date')
del k,v