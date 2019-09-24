#!~/anaconda3/bin/python

import pandas as pd
from dealsmash import *
import credentials
import psycopg2
import numpy as np
import math as m
from table_insertions import *

# Connecting to prod database using psycopg2
config_prod = credentials.config_prod
con_prod = psycopg2.connect(**config_prod)
cursor_prod = con_prod.cursor()

if __name__ == "__main__":

    # Extract coupon views
    df_coupon_views = pd.read_sql("SELECT * FROM re_coupon_views", con=con_prod)
    df_store_views = pd.read_sql("SELECT * FROM re_store_views", con=con_prod)
    print("The number of users who viewed a coupon ", len(df_coupon_views['user_id'].unique()))
    print("The number of coupon items rated ", len(df_coupon_views['coupon_id'].unique()))

    # Extract raw implicit ratings
    df_implicit_ratings = implicit_ratings(df_coupon_views, df_store_views)
    df_implicit_ratings = df_implicit_ratings[['user_id', 'coupon_id', 'implicit_rating']]
    users = df_implicit_ratings['user_id'].unique().tolist()

    # normalization of ratings
    a = 1
    b = 5
    x = []
    for u in users:
        user_ratings = df_implicit_ratings[df_implicit_ratings['user_id'] == u]
        implicit_rating = np.array(user_ratings['implicit_rating'])
        for i in range(0, len(implicit_rating)):
            x_new = a + ((implicit_rating[i] - np.min(implicit_rating)) * (b - a)) / (
                np.max(implicit_rating) - np.min(implicit_rating))
            x_new = round(x_new, 2)
            if m.isnan(x_new) and np.min(implicit_rating) == np.max(implicit_rating):
                x.append(b)
            else:
                x.append(x_new)
    df_implicit_ratings['rating'] = np.array(x)

    # Explicit Ratings
    # Combining explicit and implicit ratings
    df_explicit_ratings = pd.read_sql("SELECT user_id, coupon_id, rating FROM re_explicit_ratings", con=con_prod)
    df_implicit_ratings = df_implicit_ratings[['user_id', 'coupon_id', 'rating']]

    df_ratings = pd.concat([df_explicit_ratings, df_implicit_ratings])
    df_ratings = df_ratings.drop_duplicates()
    df_ratings = df_ratings.groupby(['user_id', 'coupon_id']).mean().reset_index()
    df_ratings['rating'] = df_ratings['rating'].apply(lambda X: round(X))

    # if a certain user saves a coupon, increase his rating to 5 and if a certain user unsaves a coupon,
    # decrease his rating to 1 for only data from last 10 days
    # Take redemption history into account before referral users,
    # if a user has redeemed a coupon, increase the rating to 5

    # Saved Coupons
    saved_coupons = pd.read_sql("SELECT user_id, coupon_id FROM re_coupon_saved", con=con_prod)
    saved_coupons = saved_coupons.drop_duplicates()
    unsaved_coupons = pd.read_sql("SELECT user_id, coupon_id FROM re_coupon_unsaved", con=con_prod)
    unsaved_coupons = unsaved_coupons.drop_duplicates()

    # Extract saved coupons after removing coupons which have been unsaved
    saved_coupons_ids = list(set(saved_coupons['coupon_id'].get_values().tolist()).difference(
                            unsaved_coupons['coupon_id'].get_values().tolist()))

    # Extract unsaved coupons after having coupons have been unsaved
    unsaved_coupons_ids = list(set(saved_coupons['coupon_id'].get_values().tolist()).intersection(
                            unsaved_coupons['coupon_id'].get_values().tolist()))

    saved_coupons = saved_coupons[saved_coupons['coupon_id'].isin(saved_coupons_ids)]
    saved_coupons['rating'] = 5
    unsaved_coupons = unsaved_coupons[unsaved_coupons['coupon_id'].isin(unsaved_coupons_ids)]
    unsaved_coupons['rating'] = 1

    df_ratings = pd.concat([df_ratings, saved_coupons])
    df_ratings = pd.concat([df_ratings, unsaved_coupons])
    df_ratings = df_ratings.drop_duplicates()
    df_ratings = df_ratings.groupby(['user_id', 'coupon_id'])['rating'].max().reset_index()

    df_ratings['user_id'] = [int(row['user_id']) for index, row in df_ratings.iterrows()]
    df_ratings['coupon_id'] = [int(row['coupon_id']) for index, row in df_ratings.iterrows()]
    df_ratings['rating'] = [int(row['rating']) for index, row in df_ratings.iterrows()]

    data = [tuple(row) for index, row in df_ratings.iterrows()]
    data = tuple(data)

    cursor_prod.close()

    print('Dropping data from the table before bulk inserting since it is faster than bulk update or bulk upsert')
    delete_unified_ratings(config_prod)

    print('Bulk inserting unified ratings')
    insert_unified_ratings(data, config_prod)













