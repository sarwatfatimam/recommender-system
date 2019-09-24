#!~/anaconda3/bin/python

import datetime
import pandas as pd
import numpy as np
from ast import literal_eval
import math as m


# for extracting time from db date
def extract_time(series):
    temp = pd.DatetimeIndex(series)
    return temp.time


# for extracting date from db date
def extract_date(series):
    temp = pd.DatetimeIndex(series)
    return temp.date


# for converting a string date to datetime
def convert_date(string_date):
    d = datetime.datetime.strptime(string_date, '%Y-%m-%d').date()
    return d


# for extracting yesterday's date
def yesterday_date(n):
    ydate = datetime.date.fromordinal(datetime.date.today().toordinal() - n)
    return ydate


# for extracting biweekly date
def biweekly_date(start_date, n):
    bdate = datetime.date.fromordinal(start_date.toordinal() - n)
    return bdate


# for getting previous date
def diff_date(pdate, n):
    previous_date = datetime.date.fromordinal(pdate.toordinal() - n)
    return previous_date


# for getting future date
def add_date(fdate, n):
    future_date = datetime.date.fromordinal(fdate.toordinal() + n)
    return future_date


# for getting unique values from a column
def unique_values(series):
    new_series = series.unique()
    return new_series


# for getting values from a column
def getting_values(series):
    new_series = list(series.get_values())
    if len(new_series) == 1:
        new_series = new_series[len(new_series)-1]
        return new_series
    else:
        return new_series


# for extracting information between specific dates
def extract_range_information(df, column, start_value, end_value):
    df = df[(df[column] >= start_value) & (df[column] <= end_value)]
    return df


# Extract information at a specific value
def extract_single_information(df, column, value):
    df = df[df[column] == value]
    return df


# Calculating stats: total count
def total_stats(df, column_name):
    total_df = len(df[column_name].unique())
    return total_df


# yesterday's stats
def yesterday_stats(df, column_name):
    ydate = yesterday_date(n=1)
    yesterday_count = len(df[df[column_name] == ydate])
    return yesterday_count


# average over some number
def average_stats(df, avg_n):
    count = len(df)
    average = round(count/avg_n,2)
    return average


# getting the last value of a dataframe
def last_value(df, column_name, n):
    value = df[column_name].tail(n).get_values()
    if len(value) == 1:
        value = value[len(value)-1]
    return value


# extracting calender dates
def cal_dates(start_date, end_date):
    dates = pd.DataFrame({'Calender Dates': pd.date_range(start=start_date, end=end_date)})
    temp = pd.DatetimeIndex(dates['Calender Dates'])
    dates['Dates'] = temp.date
    return dates


def cal_dates_count(start_date, end_date):
    dates = pd.DataFrame({'Calender Dates': pd.date_range(start=start_date, end=end_date)})
    temp = pd.DatetimeIndex(dates['Calender Dates'])
    dates['Dates'] = temp.date
    return len(dates)


# per day count
def per_day_count(date_series, df, column_date, count_column):
    count = []
    Date = []
    ids = []

    for d in date_series:
        df_d = df[df[column_date] == d]
        consumer_id = df_d['Consumer ID'].unique()
        df_one = np.sum(df_d[count_column])
        ids.append(consumer_id)
        if df_one != 0:
            count_per_day = df_one
            count.append(count_per_day)
        else:
            count_per_day = 0
            count.append(count_per_day)
        Date.append(d)

    df_new = pd.DataFrame({'Dates': Date, 'Count/day': count, 'Consumer IDs': ids})
    return df_new


# convert string series into numeric
def convert_literal(series):
    ids = (series).apply(literal_eval).sum()
    return ids


# Convert timestamp to date
def convert_timestamp(temp):
    temp = pd.to_datetime(temp)
    temp = temp.to_pydatetime(temp)
    temp = temp.date()
    return temp


# Returns a dataframe calculating frequencies
def dataframe_frequencies(df, column1, column2):
    df_new = pd.DataFrame({column2 + '_frequency':
                          df.groupby([column1, column2]).size()}).reset_index()
    return df_new


# Returns merged dataframe
def dataframe_merging(df1, df2, column1, column2):
    df_new = pd.merge(df1, df2, how='left', left_on=[column1, column2], right_on=[column1, column2])
    return df_new


# Scaled Ratings
def scaled_ratings(implicit_rating, a, b):

    x = []
    for i in range(0, len(implicit_rating)):
        x_new = a + ((implicit_rating[i] - np.min(implicit_rating)) * (b - a)) / (
                np.max(implicit_rating) - np.min(implicit_rating))
        x_new = round(x_new, 2)
        if m.isnan(x_new) and np.min(implicit_rating) == np.max(implicit_rating):
            x.append(b)
        else:
            x.append(x_new)
    return x


# This function takes in dataframe with coupon views and outputs the raw implicit ratings
# based on frequencies of selected criterias
def implicit_ratings(df_coupon, df_store):

    # Calculating frequencies for each criteria from same table
    df_user_coupon = dataframe_frequencies(df_coupon, 'user_id', 'coupon_id')
    df_coupon_category = dataframe_frequencies(df_coupon, 'user_id', 'category_id')
    df_coupon_discount = dataframe_frequencies(df_coupon, 'user_id', 'discount')
    df_coupon_retailer = dataframe_frequencies(df_coupon, 'user_id', 'retailer_id')
    df_coupon_store = dataframe_frequencies(df_store, 'user_id', 'retailer_id')

    # extract suitable ids from implicit ratings
    df_implicit_ratings = df_coupon[['user_id', 'coupon_id', 'category_id', 'retailer_id', 'discount']]
    df_implicit_ratings = df_implicit_ratings.drop_duplicates()

    # Merge frequencies and corresponding ids
    df_implicit_ratings = dataframe_merging(df_implicit_ratings, df_user_coupon, 'user_id', 'coupon_id')
    df_implicit_ratings = dataframe_merging(df_implicit_ratings, df_coupon_category, 'user_id', 'category_id')
    df_implicit_ratings = dataframe_merging(df_implicit_ratings, df_coupon_discount, 'user_id', 'discount')
    df_implicit_ratings = dataframe_merging(df_implicit_ratings, df_coupon_store, 'user_id', 'retailer_id')
    df_implicit_ratings = dataframe_merging(df_implicit_ratings, df_coupon_retailer, 'user_id', 'retailer_id')

    # Filling zeros where frequencies are not available
    df_implicit_ratings.fillna(0, inplace=True)

    # Adding retailer frequencies because it is available both from coupon views and coupon ratings
    df_implicit_ratings['retailer_frequency'] = df_implicit_ratings['retailer_id_frequency_x'] + df_implicit_ratings[
        'retailer_id_frequency_y']

    # Getting coupon ratings
    df_implicit_ratings = df_implicit_ratings[['user_id', 'coupon_id', 'coupon_id_frequency',
                                               'category_id_frequency', 'discount_frequency',
                                               'retailer_frequency']]

    # Taking average of retailer frequency
    df_implicit_ratings = df_implicit_ratings.groupby(['user_id', 'coupon_id', 'coupon_id_frequency',
                                                       'category_id_frequency',
                                                       'discount_frequency']).mean().reset_index()

    # Taking average of frequencies
    df_implicit_ratings['implicit_rating'] = df_implicit_ratings[['coupon_id_frequency', 'category_id_frequency',
                                                                  'discount_frequency', 'retailer_frequency']].mean(axis=1)
    return df_implicit_ratings

