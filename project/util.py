import os
from pathlib import Path
import functools
import warnings

import quandl
import json
import pandas as pd
pd.set_option("display.precision", 4)
pd.set_option('display.float_format', lambda x: '%.4f' % x)
# import pandas_datareader.data as pdr

# import math
import numpy as np
import datetime as dt
from dateutil.relativedelta import relativedelta

# plotting packages
from matplotlib import pyplot as plt
plt.rcParams['figure.figsize'] = [21, 8]

# Get personal API key** from ../data/APIs.json
f = open('../data/APIs.json')
APIs = json.load(f)
f.close()



# Helper functions

def assertCorrectDateFormat(date_text):
    try:
        dt.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect date format, should be YYYY-MM-DD")

def calcSixMonthsAgo(date_text):
    assertCorrectDateFormat(date_text)
    d = dt.datetime.strptime(date_text, '%Y-%m-%d')
    return (d + relativedelta(months=-6)).strftime('%Y-%m-%d')

def calcNextMonth(month_text):
    if type(month_text) == str:
        m = dt.datetime.strptime(month_text, '%Y-%m')
    else: m = month_text
    return (m + relativedelta(months=1)).strftime('%Y-%m')

def deleteCSV(sec):
    file_name = "../data_large/EOD/"+sec
    if os.path.isfile(file_name):
        os.remove(file_name)



# Function that retrieves EOD data from Quandl
@functools.lru_cache(maxsize=16) # Cache the function output
def getQuandlEODData(sec,start_date,end_date,columns):
    # Get one security (sec)'s data fom Quandl using quandl.get_table
    # NOTE: missing data for the inputted date will NOT return a row.

    # INPUT         | DATA TYPE                 | DESCRIPTION
    # sec           | string / list of string   | security ticker
    # start_date    | string (YYYY-MM-DD)       | start date of data
    # end_date      | string (YYYY-MM-DD)       | end date of data (same as or after start_date)
    # columns       | string / list of string   | columns to return
    
    print(f"Quandl | START | Retriving Quandl data for security: {sec}\n")
    
    # Retrieve data using quandl.get_table
    quandl.ApiConfig.api_key = APIs['Quandl']
    data = quandl.get_table('QUOTEMEDIA/PRICES',
                            ticker = sec, 
                            date = {'gte':start_date, 'lte':end_date},
                            qopts = {'columns':list(set(['date','ticker']+list(columns)))}
                            )

    data.date = pd.to_datetime(data.date, unit='D')
    data.dropna(inplace=True)
    
    print(f"Quandl | DONE  | Returning {len(data):d} dates of data from {data.date.min()} to {data.date.max()}.\n")
    
    data.set_index(['date','ticker'],inplace=True)
    data.sort_index(inplace=True)
    
    return data