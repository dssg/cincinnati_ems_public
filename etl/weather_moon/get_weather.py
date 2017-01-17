import requests
import os
import errno
import time
import pandas as pd
import numpy as np
from weather_creds import API_KEY, API_KEY2


def mkdir_p(path):
    """ If a directory does not exist, create it. """
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: 
            raise


def get_weather_data(startdate, enddate, api_key, stationid, datatype=None,
                     datasetid='GHCND', units='standard', limit=1000, 
                     return_request=False):
    """
    TODO

    NOTES:
    http://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt
    https://www.ncdc.noaa.gov/cdo-web/token

    """

    # Set up the URL, parameters, and header
    url = 'http://www.ncdc.noaa.gov/cdo-web/api/v2/data'
    payload = {'datasetid': datasetid, 
               'stationid': stationid, 
               'startdate': startdate, 
               'enddate': enddate,
               'units': units, 
               'limit': limit, 
               'includemetadata': 'true'}
    # if datatype is not None:
     #    payload['datatypeid'] = datatype
    headers = {'token': api_key}
    
    # Send a request and check it
    r = requests.get(url, params=payload, headers=headers)
    
    if return_request:
        return r

    # Did the request go through?
    if r.ok:
        # Does data exist?
        if r.json():
            # Convert to dataframe and return
            df = pd.io.json.json_normalize(r.json()['results'])
            df.date = pd.to_datetime(df.date)
            return df
        else:
            pass
    else:
        raise Exception("Bad request. Error code: {}".format(r.status_code))


if __name__=="__main__":
    cinci_stations = ['GHCND:US1KYKN0001',
                      'GHCND:US1KYKN0012',
                      'GHCND:USW00093812',
                      'GHCND:US1OHHM0017',
                      'GHCND:US1OHHM0005',
                      'GHCND:USC00331515']
    
    data_dir = '/mnt/data/cincinnati/weather_data'
    mkdir_p(data_dir)

    for station in cinci_stations:
        # Get 2007 to end of 2015
        for year in range(2007, 2016):
            # To save on API requests, don't download if file exists
            if os.path.exists('{}/weather_{}_{}.csv'.format(data_dir, year, 
                                                            station[6:])):
                pass
            df_container = []
            ends = pd.date_range(str(year), str(year + 1), freq='M')
            starts = ends.shift(-1, freq=pd.datetools.MonthBegin())

            for i, _ in enumerate(starts): 
                print("Downloading {}: {} to {}.".format(station, 
                                                         starts[i], ends[i]))
                df = get_weather_data(starts[i].strftime('%Y-%m-%d'), 
                                      ends[i].strftime('%Y-%m-%d'), 
                                      API_KEY2, station)
                df_container.append(df)
                
                # Be nice to the API
                time.sleep(np.random.randint(1, 5))
            
            # If any data was downloaded at all 
            if len(df_container) != sum(x is None for x in df_container):
                year_weather = pd.concat(df_container)
                year_weather.reset_index(drop=True, inplace=True)
                year_weather = year_weather[['station', 'date', 'datatype', 
                                            'value', 'attributes']]
                
                year_weather.to_csv('{}/weather_{}_{}.csv'.format(data_dir, 
                                                year, station[6:]), index=False)

        # Get first half of 2016
        ends = pd.date_range('2016-01', '2016-07', freq='M')
        starts = ends.shift(-1, freq=pd.datetools.MonthBegin())
        df_container = []
        for i, _ in enumerate(starts): 
            print("Downloading {}: {} to {}.".format(station, 
                                                     starts[i], ends[i]))
            df = get_weather_data(starts[i].strftime('%Y-%m-%d'), 
                                  ends[i].strftime('%Y-%m-%d'), 
                                  API_KEY2, station)
            df_container.append(df)
            time.sleep(np.random.randint(1, 5))
            
            # If any data was downloaded at all 
            if len(df_container) != sum(x is None for x in df_container):
                year_weather = pd.concat(df_container)
                year_weather.reset_index(drop=True, inplace=True)
                year_weather = year_weather[['station', 'date', 'datatype', 
                                             'value', 'attributes']]
                
                year_weather.to_csv('{}/weather_{}_{}.csv'.format(data_dir, 
                                                '2016a', station[6:]), index=False)
