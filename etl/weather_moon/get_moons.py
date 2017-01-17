import requests
import errno
import pandas as pd
import os


def mkdir_p(path):
    """ If a directory does not exist, create it. """
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def get_lunar_phases(starting_date, n_phase=48, full_moon_only=False):
    """ Returns a dataframe containing lunar phases from specified date

    This function uses the US Military API to get lunar phase data. Returns
    a pandas dataframe with the date as the index. Will return a dataframe with
    three columns (date, phase, time) and n_phase rows.

    :param str starting_date: the starting date of the data in 'MM/DD/YYYY'
    :param int n_phase: the number of primary phases (4 per lunar cycle)
    :param bool full_moon_only: return a df with only full moons

    """
    # API limits number of phases you can request
    if n_phase > 96:
        raise Exception("n_phase must be less than 96")

    # Make request and convert JSON
    url = 'http://api.usno.navy.mil/moon/phase' 
    payload = {'date': starting_date, 
               'nump': n_phase}
    
    r = requests.get(url, params=payload)

    if r.ok:
        df = pd.io.json.json_normalize(r.json()['phasedata'])
        df.date = pd.to_datetime(df.date)
        df.set_index('date', inplace=True)
    else:
        raise Exception("Bad request. Error code: {}".format(r.status_code))

    if full_moon_only is True:
        return df[df.phase == 'Full Moon']

    return df

if __name__=="__main__":
    data_dir = '/mnt/data/cincinnati/moon'
    mkdir_p(data_dir)

    df_container = []
    for year in range(2007, 2026):
        temp_df = get_lunar_phases('1/1/' + str(year), n_phase=55)
        df_container.append(temp_df)
    
    df = pd.concat(df_container)
    df.drop_duplicates(inplace = True)

    df.to_csv('{}/lunar_phases.csv'.format(data_dir))
