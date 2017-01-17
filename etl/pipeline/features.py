import utils.pg_tools as pg
import pandas as pd
from datetime import timedelta
import numpy as np

pgw = pg.PGWrangler(dbitems = pg.get_pgdict_from_cfg())

def time_delayed_feature(df, feature, tdelta, aggby=np.median, group=None,
                         roll=1):
    """
    Create a delayed version of a variable as a feature by aggregating the
    feature per day and shift it

    :param pd.DataFrame df: DataFrame with feature to be delayed
    :param str feature: feature to be delayed
    :param timedelta tdelta: time by which to shift feature (in days)
    :param func aggby: function by which to aggregate feature over incidents for a day
    :param str group: additional feature by which to discriminate (e.g. fire station, code...)
    :param int roll: number of days over which to accumulate feature (rolling sum up until day-tdelta)
    :returns: DataFrame with delayed feature as column
    """
    df['_date'] = pd.to_datetime(df['time_created'].apply(lambda x: x.date()))
    groups = ['_date']
    if group:
        groups.append(group)
        all_group = df[group].unique()
    agg_feature = df[groups + [feature]].groupby(groups)[feature].agg(aggby)
    # Impute first values with value at min date
    min_date = agg_feature.index.min()
    if group:
        min_date = min_date[0]
    for date in [min_date - timedelta(days=x) for x in range(1,tdelta.days+1)]:
        if group:
            for g in all_group:
                try:
                    agg_feature[(date,g)] = agg_feature[min_date][g]
                except KeyError:
                    agg_feature[(date,g)] = 0
        else:
            agg_feature[date] = agg_feature[min_date]
    # Apply rolling sum up until t-tdelta if roll>1
    if roll>1 and group:
        def rollsum(g):
            old_idx = g.index.copy()
            tmp = g.reset_index().set_index('_date')
            idx = tmp.index.copy()
            result = pd.rolling_sum(tmp.resample("1D").sum().fillna(0), roll, min_periods=0).ix[idx]
            result.set_index(old_idx, inplace=True)
            return result
        agg_feature = agg_feature.groupby(level=group).apply(rollsum).sum(axis=1)
    if roll>1 and not group:
        agg_feature = pd.rolling_sum(agg_feature, roll, min_periods=0)
    # Helper function to return 0 if feature not defined for delay
    # (e.g. if specific fire station, code type had no feature on that date)
    def old_feature(row):
        if group:
            try:
                return agg_feature[row['_date']-tdelta][row[group]]
            except:
                return 0
        else:
            try:
                return agg_feature[row['_date']-tdelta]
            except:
                return 0

    delayed_feature = df[groups].apply(old_feature, axis=1)
    # Drop helper data
    df.drop('_date', axis=1)
    return delayed_feature

def relative_max_temperature_feature(df):
    tdelta = timedelta(days=7)
    feature = 'common_weather_tmax'
    told = time_delayed_feature(df, feature, tdelta)
    return df[feature]-told

def relative_min_temperature_feature(df):
    tdelta = timedelta(days=7)
    feature = 'common_weather_tmin'
    told = time_delayed_feature(df, feature, tdelta)
    return df[feature]-told

def frac_trans_1day_feature(df):
    return time_delayed_feature(df, 'trns_to_hosp', timedelta(days=1), 
                                aggby=np.mean)

def frac_trans_1week_feature(df):
    # Fraction transported 7 days ago
    return time_delayed_feature(df, 'trns_to_hosp', timedelta(weeks=1),
                                aggby=np.mean)

def total_transports_1month_feature(df):
    # Total transports over the last month
    return time_delayed_feature(df, 'trns_to_hosp', timedelta(days=1),
                                aggby=np.sum, roll=30)

def total_transports_1day_bystation_feature(df):
    return time_delayed_feature(df, 'trns_to_hosp', timedelta(days=1),
                                aggby=np.sum, group='station_name', roll=1)

def total_transports_lastweek_bystation_feature(df):
    return time_delayed_feature(df, 'trns_to_hosp', timedelta(days=1),
                                aggby=np.sum, group='station_name', roll=7)

def total_transports_lastweek_bycode_feature(df):
    return time_delayed_feature(df, 'trns_to_hosp', timedelta(days=1),
                                aggby=np.sum, group='code_type', roll=7)

def total_transports_lasthour_feature(df):
    return time_delayed_feature(df, 'trns_to_hosp', timedelta(hours=1),
                                aggby=np.sum)

def building_type_feature(df):
    return df['call_type'].fillna('other')

def time_of_day_feature(df):
    def tod(time):
        if time >= 7 and time < 13:
            return "7a-1p"
        if time >= 13 and time < 19:
            return "1p-7p"
        if time >= 19 or time < 1:
            return "7p-1a"
        else:
            return "1a-7a"
    return df['time_hour'].apply(tod)


def weekday_feature(df):
    """
    Compute the weekday from time_created

    :param pd.DataFrame df: DataFrame with time_created column
    :returns: weekday of creation
    :rtype: pd.Series
    """
    return df['time_created'].apply(lambda x: x.weekday())

def trns_or_refused_feature(df):
    def refused(x):
        try:
            return ("refused transport" in x) or ("refusal" in x)
        except:
            return False
    refused = df['comments'].apply(refused)
    return df['trns_to_hosp'] | refused

def acs_feature(df, table_name, column_expression, feature_name, start_year):
    """
    Selects an acs feature

    :param str table_name: name of acs table
    :param str column_expression: column-expression to evaluate in select
    :param str feature_name: name of feature in df to return
    :param int start_year: year in which to start evaluation
    :returns: pandas df indexed by incident with feature_name as a column
    :rtype: pd.DataFrame
    """
    # Expects time_year and geoid as a feature in df
    # Get all geoids and years
    geoids = df['geoid'].unique()
    years = df['time_year'].unique().astype(int)
    # Create df between geoid, year to income
    dfs = []
    for year in years:
        query_year = year-2 # Eric: only -2 year census data at each time
        # Eric: different geoids for 2009 and earlier
        if query_year < start_year:
            query_year = start_year
        sql = ("select %s as %s, substring(geoid from 8) as geoid "
               "from acs%d_5yr.%s"%(column_expression, feature_name, 
                                        query_year, table_name))
        conn = pgw.get_conn()
        df_year = pd.read_sql(sql, conn)
        conn.close()
        df_year['year'] = year
        dfs.append(df_year)
    feature_df = pd.concat(dfs, ignore_index=True)
    pre_merged = df[['time_year','geoid']].reset_index()
    merged = pre_merged.merge(feature_df, how='left',
                              left_on=['time_year','geoid'],
                              right_on=['year','geoid']
                              ).set_index('incident')
    return merged[feature_name]


def acs_income_feature(df):
    return acs_feature(df, 'B19013', 'b19013001', 'acs_income', 2010)

def acs_age_feature(df):
    return acs_feature(df, 'B01002', 'b01002001', 'acs_age', 2010)

def acs_no_insurance_feature(df):
    expression = ("COALESCE("
                  "(b27010017+ "#18yr
                  "b27010033+ " #18-34yr
                  "b27010050+ " #35-64yr
                  "b27010066)/NULLIF(b27010001,0), 0)") #65+yr
    return acs_feature(df, 'B27010', expression, 'acs_no_insurance', 2013)

def acs_edu_feature(df):
    expression = ("COALESCE("
                  "(B15003022+ " #bachelors
                  "B15003023+ " #masters
                  "B15003024+ " #prof.school
                  "B15003025)/NULLIF(B15003001,0), 0)") #doctorate 
    return acs_feature(df, 'B15003', expression, 'acs_edu', 2012)

def acs_white_feature(df):
    return acs_feature(df, 'B02001',
                       'COALESCE(b02001002/NULLIF(b02001001,0),0)',
                       'acs_white', 2010)

def acs_black_feature(df):
    return acs_feature(df, 'B02001',
                       'COALESCE(b02001003/NULLIF(b02001001,0), 0)',
                       'acs_black', 2010)

# mkiang features start here
def within_1day_full_moon_feature(df):
    """
    Finds all days that are within +/- 1 day of a full moon and returns the df.
    """
    conn = pgw.get_conn()
    moon = pd.read_sql_query('select * from external.moon',
                             con = conn)
    conn.close()
    full_dates = moon[moon.phase == 'Full Moon'].date
    set0 = set(full_dates)
    set1 = set(full_dates + pd.DateOffset(1))
    set2 = set(full_dates + pd.DateOffset(-1))

    full_moons = set0 | set1 | set2
    
    def within_day(x):
        if (pd.datetime(int(x.time_year), 
                        int(x.time_month), 
                        int(x.time_day)) in full_moons):
            return True
        else:
            return False

    return df.apply(within_day, axis=1)


def common_weather_feature(df):
    """
    Returns common weather (temp, wind, etc.)
    """
    # TODO ADD DICTIONARY TO MAKE COLUMN NAMES MORE UNDERSTANDABLE

    # Find appropriate date range
    min_date = df.time_created.min().strftime('%Y-%m-%d')
    max_date = df.time_created.max().strftime('%Y-%m-%d')

    # Get non-weird weather events within that date range
    conn = pgw.get_conn()
    weather = pd.read_sql_query(("select date, datatype, value from " + 
                                 "external.weather where date <= '{}' ".format(max_date) + 
                                 "and date >= '{}' ".format(min_date) + 
                                 "and substr(datatype, 1, 2) <> 'WT'"), 
                                con = conn)
    conn.close()
    weather['date'] = pd.to_datetime(weather['date'])
    
    # Group, pivot, flatten, make better column names
    grouped_means = weather.groupby(['date', 'datatype']).mean()
    grouped_means = grouped_means.unstack()['value']
    grouped_means.columns = grouped_means.columns.str.lower()

    # Merge
    df['date'] = df['time_created'].apply(lambda x: x.strftime('%Y-%m-%d'))
    df['date'] = pd.to_datetime(df.date)
    to_return = df.join(grouped_means, on='date')[grouped_means.columns]
    to_return.fillna(0, inplace=True)

    # Avoid side effects by making sure df looks the same as when received
    df.drop('date', axis=1, inplace=True)

    return to_return


def weather_event_feature(df):
    """
    Returns uncommon weather events (hail, freezing rain, etc.)
    """

    # Find appropriate date range
    min_date = df.time_created.min().strftime('%Y-%m-%d')
    max_date = df.time_created.max().strftime('%Y-%m-%d')

    # Get non-weird weather events within that date range
    conn = pgw.get_conn()
    weather = pd.read_sql_query(("select date, datatype, value from " + 
                                 "external.weather where date <= '{}' ".format(max_date) + 
                                 "and date >= '{}' ".format(min_date) + 
                                 "and substr(datatype, 1, 2) = 'WT'"), 
                                con = conn)
    conn.close()
    weather['date'] = pd.to_datetime(weather['date'])
    
    # Dictionary from 
    # ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt
    event_dict = {'WT01': 'ice_fog', 
                  'WT02': 'heavy_fog', 
                  'WT03': 'thunder', 
                  'WT04': 'ice_pellets', 
                  'WT05': 'hail', 
                  'WT06': 'glaze', 
                  'WT07': 'dust', 
                  'WT08': 'smoke', 
                  'WT09': 'drifting_snow', 
                  'WT10': 'tornado', 
                  'WT11': 'high_winds', 
                  'WT12': 'blowing_spray', 
                  'WT13': 'mist', 
                  'WT14': 'drizzle', 
                  'WT15': 'freezing_drizzle', 
                  'WT16': 'rain', 
                  'WT17': 'freezing_rain', 
                  'WT18': 'snow', 
                  'WT19': 'unknown_precip', 
                  'WT21': 'ground_fog', 
                  'WT22': 'ice_fog'}
    

    # Replace with our new dict
    weather.datatype.replace(event_dict, inplace=True)
    
    # Group, pivot, flatten, make better column names
    grouped_means = weather.groupby(['date', 'datatype']).mean()
    grouped_means = grouped_means.unstack()['value']
    grouped_means.fillna(0.0, inplace=True)

    # Parse date so we can merge -- must delete this column later!
    df['date'] = df['time_created'].apply(lambda x: x.strftime('%Y-%m-%d'))
    df['date'] = pd.to_datetime(df.date)

    # Merge and then fix `df` so we don't get side-effects
    to_return = df.join(grouped_means, on='date')[grouped_means.columns]
    to_return.fillna(0, inplace=True)
    
    df.drop('date', axis=1, inplace=True)

    return to_return

def temp_above_feature(df, temp_thresh=90):
    """
    Returns bool for days when average max temperature is above some value.
    """

    # Find appropriate date range
    min_date = df.time_created.min().strftime('%Y-%m-%d')
    max_date = df.time_created.max().strftime('%Y-%m-%d')

    # Get non-weird weather events within that date range
    conn = pgw.get_conn()
    temps = pd.read_sql_query(("select date, value from " + 
                                "external.weather where date <= '{}' ".format(max_date) + 
                                "and date >= '{}' ".format(min_date) + 
                                "and datatype = 'TMAX'"), 
                               con = conn)
    temps['date'] = pd.to_datetime(temps['date'])
    conn.close()

    grouped_temps = temps.groupby(['date']).max()
    grouped_temps['temp_above'] = grouped_temps >= temp_thresh

    df['date'] = df['time_created'].apply(lambda x: x.strftime('%Y-%m-%d'))
    df['date'] = pd.to_datetime(df.date)

    to_return = df.join(grouped_temps, on='date')['temp_above']

    # Avoid side effects by making sure df looks the same as when received
    df.drop('date', axis=1, inplace=True)

    return to_return


def temp_below_feature(df, temp_thresh=25):
    """
    Returns bool for days when average min temperature is below some value.
    """

    # Find appropriate date range
    min_date = df.time_created.min().strftime('%Y-%m-%d')
    max_date = df.time_created.max().strftime('%Y-%m-%d')

    # Get non-weird weather events within that date range
    conn = pgw.get_conn()
    temps = pd.read_sql_query(("select date, value from " + 
                                "external.weather where date <= '{}' ".format(max_date) + 
                                "and date >= '{}' ".format(min_date) + 
                                "and datatype = 'TMIN'"), 
                               con = conn)
    conn.close()
    temps['date'] = pd.to_datetime(temps['date'])
    
    grouped_temps = temps.groupby(['date']).min()
    grouped_temps['temp_below'] = grouped_temps <= temp_thresh

    # Merge
    df['date'] = df['time_created'].apply(lambda x: x.strftime('%Y-%m-%d'))
    df['date'] = pd.to_datetime(df.date)
    to_return = df.join(grouped_temps, on='date')['temp_below']

    # Avoid side effects by making sure df looks the same as when received
    df.drop('date', axis=1, inplace=True)

    return to_return

#karen features start here
def incident_duration_feature(df):
    """
    calculated the duration in minutes of the incident 
    from the time it was created until the time it closed

    :returns: a column containing the duration in minutes  
    :rtype: [float]
    """
    df["duration"] = df.apply(lambda incident:
                              (incident.time_closed
                               - incident.time_created).total_seconds()/60,
                              axis=1)  
    return (df.duration) 

def operator_experience_feature(df):
    """
    caclulates the number of incidents the operator is signed for from 2011

    returns: a colomn containing the number of incidents from 2011 this operator is signed for
    rtype: [float]
    """
    return df.groupby('operator_name')['operator_name'].cumcount()

def time_weekend_feature(df):
    """
    checks if an incident was during the weekend

    reutrns: a colomn indictating whether it is the weekend (Saturday \ Sunday) or not
    rtype: [bool]
    """
    import datetime

    return df.apply(lambda x: True if
                        datetime.datetime(int(x.time_year),
                                          int(x.time_month),
                                          int(x.time_day)).weekday() > 4
                        else False, axis=1)


def repeated_loc_24h_feature(df):
    """
    fetches the data from the previosuly created features

    """
    conn = pgw.get_conn()
    same_address = pd.read_sql_query('select incident,same_address from '
                                     'expensive_features.same_add_24h',
                                     con = conn)
    conn.close()
   
    same_address.set_index("incident",inplace=True)
    assert(all(same_address.index == df.index) and len(same_address) == len(df))
    return(same_address)

def lag_24h_station_incidents_feature(df):
    """
    Not tested
    """
    conn = pgw.get_conn()
    lag = pd.read_sql_query('select incident,same_station from '
                                     'expensive_features.lg_24h_same_station',
                                     con = conn)
    conn.close()
    lag.set_index("incident",inplace=True)
    assert(all(lag.index == df.index) and len(lag) == len(df))
    return(lag)
    
def lag_24h_station_trns_feature(df):
    """
    Not tested
    """
    conn = pgw.get_conn()
    lag = pd.read_sql_query('select incident,same_station_trns from '
                                     'expensive_features.lg_24h_same_station_trns',
                                     con = conn)
    conn.close()
    lag.set_index("incident",inplace=True)
    assert(all(lag.index == df.index) and len(lag) == len(df))
    return(lag)

def lag_1w_station_trns_feature(df):
    """
    Not tested
    """
    conn = pgw.get_conn()
    lag = pd.read_sql_query('select incident,same_station_trns from '
                                     'expensive_features.lg_1w_same_station_trns',
                                     con = conn)
    conn.close()
    lag.set_index("incident",inplace=True)
    assert(all(lag.index == df.index) and len(lag) == len(df))
    return(lag)

def lag_1w_station_incidents_feature(df):
    """
    Not tested
    """
    conn = pgw.get_conn()
    lag = pd.read_sql_query('select incident,same_station from '
                                     'expensive_features.lg_1w_same_station',
                                     con = conn)
    conn.close()
    lag.set_index("incident",inplace=True)
    assert(all(lag.index == df.index) and len(lag) == len(df))
    return(lag)


def is_holiday_feature(df):

    import datetime

    def is_holiday(t):
        """
        input: a datetime object
        output: whether this date is a federal or state holiday
        """
        if t.month == 12 and t.day == 25:
            #Christmas
            return True
        elif t.month == 1 and t.day == 1:
            #New Years Day
            return True
        elif t.month == 7 and t.day == 4:
            #Independence Day
            return True
        elif t.month == 1 and t.weekday() == 0 and t.day <= 21 and t.day >= 15:
            #MLK Day - 3rd monday in Jan
            return True
        elif t.month == 2 and t.weekday() == 0 and t.day <= 21 and t.day >= 15:
            #President's Day - 3rd Monday in Feb
            return True
        elif t.month == 5 and t.weekday() == 0 and t.day >= 25:
            #Memorial Day - Last Monday in May
            return True
        elif t.month == 9 and t.weekday() == 0 and t.day <= 8:
            #Labor Day - 1st Monday in Sept.
            return True
        elif t.month == 10 and t.weekday() == 0 and t.day <= 14 and t.day >= 8:
            #Columbus Day - 2nd Monday in Oct.
            return True
        elif t.month == 11 and t.weekday() == 3 and t.day <= 28 and t.day >= 22:
            #Thanksgiving - 4th Thurs in Nov.
            return True
        elif (t-datetime.timedelta(1)).month == 11 and (t-datetime.timedelta(1)).weekday() == 3 and (t-datetime.timedelta(1)).day <= 28 and (t-datetime.timedelta(1)).day >= 22:
            #Thanksgiving - 4th Thurs in Nov.
            return True
        if t.month == 12 and (t.day == 27 or t.day == 26) and t.weekday() == 0:
            #Monday after Christmas if Christmas on a weekend
            return True
        return False

    return df['_date'].apply(is_holiday)
