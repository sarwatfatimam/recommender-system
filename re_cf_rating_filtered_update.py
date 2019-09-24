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

    control_group = pd.DataFrame.from_csv('ControlGroupData.csv')
    control_group_ids = control_group['user_id'].tolist()
    control_group = tuple(control_group_ids)

    print('Removing Control Group IDs')
    delete_control_group(control_group, config_prod)

    df_re_users = pd.read_sql('SELECT DISTINCT user_id FROM re_cf_rating_master', con=con_prod)
    user_ids = df_re_users['user_id'].tolist()

    print('Dropping data from the table before bulk inserting since it is faster than bulk update or bulk upsert')
    delete_cf_ratings(config_prod)

    start_index = 0
    end_index = start_index + 2500
    length = round(len(user_ids) / 2500)

    for i in range(0, length + 1):
        print('LOOP', i)
        u = user_ids[start_index:end_index]
        user_list = tuple(u)
        # Filter recommendations based on
        # 1. Coupon is assigned to the user
        # 2. At least one retailer branch of the coupon is enabled
        # 3. Coupon expiry > today's date
        # 4. Coupon should be enabled
        if user_list:
            # production database
            con_prod = psycopg2.connect(**config_prod)
            cursor_prod = con_prod.cursor()

            query = ("SELECT DISTINCT re.user_id, "
                     "re.item_id, re.ratings, c.is_enable, c.category_id "
                     "from re_cf_rating_master re "
                     "INNER JOIN ds_coupon c on c.id = re.item_id "
                     "WHERE re.user_id IN " + str(user_list) + " ORDER BY re.ratings DESC ")

            print('Running Query')
            cursor_prod.execute(query)
            data = cursor_prod.fetchall()
            cursor_prod.close()

            df_re_user_ratings = pd.DataFrame([[ij for ij in i] for i in data])
            df_re_user_ratings.rename(columns={0: 'user_id', 1: 'item_id', 2: 'ratings',
                                               3: 'is_enable', 4: 'category_id'}, inplace=True)

            print('Length of df_re_user_ratings', len(df_re_user_ratings))

            # Taking only top three coupons from each brand
            df_new = df_re_user_ratings.groupby(['user_id', 'name']).head(3).reset_index(drop=True)
            print('Length after choosing only 3 coupons from a brand', len(df_new))
            df_new = df_new[['user_id', 'item_id', 'ratings', 'category_id']]

            print('Creating the tuple')
            data = [tuple(row) for index, row in df_new.iterrows()]
            data = tuple(data)

            print('The length of filtered data for user ', len(data))

            print('Inserting data into database')
            insert_cf_ratings(data, config_prod)

        print('Setting indexes for next loop')
        start_index = end_index
        end_index = start_index + 2500
        if end_index > len(user_ids):
            end_index = len(user_ids) + 1




