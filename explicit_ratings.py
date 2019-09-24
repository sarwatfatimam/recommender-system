#!~/anaconda3/bin/python

import pandas as pd
from dealsmash import *
import credentials
import psycopg2
from table_insertions import *

# Connecting to prod database using psycopg2
config_prod = credentials.config_prod
con_prod = psycopg2.connect(**config_prod)
cursor_prod = con_prod.cursor()

if __name__ == "__main__":

    # Extract users information
    df_user = pd.read_sql("SELECT id, first_name FROM ds_user WHERE CAST(created_at as DATE) <= now()", con=con_prod)
    removed_accounts = pd.read_sql("SELECT id FROM re_removed_accounts", con=con_prod)
    removed_ids = list(removed_accounts['id'].get_values())
    df_user = df_user[~df_user['id'].isin(removed_ids)]
    installed_users = list(df_user['id'].get_values())

    # Extract explicit ratings
    df_explicit_ratings = pd.read_sql("SELECT ac.user_id, ac.coupon_id, c.text AS coupon_text, "
                                      "c.category_id, pc.name AS category_name, "
                                      "rb.id as retailer_id, rb.name as retailer_name, rb.status as retailer_status, "
                                      "ac.rating, c.standard_coupon, "
                                      "c.discount, c.discount_y, ac.assigned_on "
                                      "FROM ds_coupon_assigned_user ac "
                                      "LEFT JOIN ds_coupon c ON ac.coupon_id = c.id "
                                      "LEFT JOIN ds_product_category pc ON c.category_id = pc.id "
                                      "LEFT JOIN ds_product p ON c.product_id = p.id "
                                      "INNER JOIN ds_coupon_multiple_branch cm ON c.id = cm.coupon_id "
                                      "LEFT JOIN ds_retailer_branch rb ON rb.id = cm.retailer_branch_id "
                                      "WHERE ac.rating IS NOT NULL and ac.rating != 0 and "
                                      "CAST(ac.assigned_on as DATE) <= now() ", con=con_prod)
    df_explicit_ratings = df_explicit_ratings[df_explicit_ratings['user_id'].isin(installed_users)]
    df_explicit_ratings = df_explicit_ratings[['user_id', 'coupon_id', 'category_id', 'retailer_id',
                                               'retailer_status', 'rating', 'standard_coupon',
                                               'discount', 'discount_y']]
    df_explicit_ratings['discount_y'].fillna(0, inplace=True)
    print('The total number of users with explicit ratings', len(df_explicit_ratings['user_id'].unique().tolist()))
    print('The total number of coupons given explicit ratings', len(df_explicit_ratings['coupon_id'].unique().tolist()))

    print('Concatenating data for bulk insert')
    data = [tuple(row) for index, row in df_explicit_ratings.iterrows()]
    data = tuple(data)

    print('Dropping data from the table before bulk inserting since it is faster than bulk update or bulk upsert')
    delete_explicit_ratings(config_prod)

    print('Bulk inserting explicit ratings data')
    insert_explicit_ratings(data, config_prod)








