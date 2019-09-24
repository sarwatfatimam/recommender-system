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

    df_store_views = pd.read_sql("SELECT ul.user_id, ul.action_id AS retailer_id, rb.name AS retailer_name, "
                                 "rb.status as retailer_status, rs.category_id, "
                                 "sc.name AS store_category, ul.is_store, ul.time FROM ds_user_logs ul "
                                 "LEFT JOIN ds_retailer_branch rb ON rb.id = ul.action_id "
                                 "LEFT JOIN ds_retailer_branch_store_categories rs "
                                 "ON ul.action_id = rs.retailer_branch_id "
                                 "LEFT JOIN ds_store_categories sc ON rs.category_id = sc.id "
                                 "WHERE is_store = TRUE and rb.status = TRUE and CAST(ul.time as DATE) <= now() ",
                                 con=con_prod)
    df_store_views = df_store_views[df_store_views['user_id'].isin(installed_users)]
    df_store_views = df_store_views[['user_id', 'retailer_id', 'retailer_status',
                                     'time']]

    print('The total number of users who viewed store', len(df_store_views['user_id'].unique().tolist()))
    print('The total number of stores viewed are', len(df_store_views['retailer_id'].unique().tolist()))

    print('Concatenating data for bulk insert')
    data = [tuple(row) for index, row in df_store_views.iterrows()]
    data = tuple(data)

    cursor_prod.close()

    print('Dropping data from the table before bulk inserting since it is faster than bulk update or bulk upsert')
    delete_store_views(config_prod)

    print('Bulk inserting store views data')
    insert_store_views(data, config_prod)