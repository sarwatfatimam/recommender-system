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

    df_re_users = pd.read_sql('SELECT DISTINCT user_id FROM re_cf_rating_master', con=con_prod)
    # control_group = pd.DataFrame.from_csv('/home/ec2-user/analysis/ControlGroupData.csv')
    # control_group_ids = control_group['user_id'].tolist()
    # df_re_users = df_re_users[~df_re_users['user_id'].isin(control_group_ids)]
    user_ids = df_re_users['user_id'].tolist()

    before_re_users = pd.read_sql('SELECT DISTINCT user_id FROM re_cf_rating', con=con_prod)
    before_re_ids = before_re_users['user_id'].get_values().tolist()
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
                     "CASE WHEN rb.name = 'D Watson F-11' THEN 'D Watson' "
                     "WHEN rb.name = 'D Watson F-10' THEN 'D Watson' "
                     "ELSE rb.name  "
                     "END, "
                     "re.item_id, c.text, re.ratings, c.is_enable, rb.status, c.category_id, rb.city "
                     "from re_cf_rating_master re "
                     "INNER JOIN ds_coupon c on c.id = re.item_id "
                     "INNER JOIN ds_coupon_multiple_branch cm ON c.id = cm.coupon_id "
                     "LEFT JOIN ds_retailer_branch rb ON rb.id = cm.retailer_branch_id "
                     "INNER JOIN ds_user_selected_city uc on uc.user_id = re.user_id AND rb.city = uc.city_id "
                     "INNER JOIN ds_coupon_assigned_user cu "
                     "ON cu.user_id = re.user_id AND cu.coupon_id = re.item_id "
                     "WHERE re.user_id IN " + str(user_list) + "and c.is_enable = TRUE and rb.status = True "
                     "and CAST(c.expiry as DATE) > now() ORDER BY re.ratings DESC ")

            print('Running Query')
            cursor_prod.execute(query)
            data = cursor_prod.fetchall()
            cursor_prod.close()

            df_re_user_ratings = pd.DataFrame([[ij for ij in i] for i in data])
            df_re_user_ratings.rename(columns={0: 'user_id', 1: 'name', 2: 'item_id', 3: 'text', 4: 'ratings',
                                    5: 'is_enable', 6: 'status', 7: 'category_id'}, inplace=True)

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

    after_re_users = pd.read_sql('SELECT DISTINCT user_id FROM re_cf_rating', con=con_prod)
    after_re_ids = after_re_users['user_id'].get_values().tolist()
    new_re_users = list(set(after_re_ids).difference(set(before_re_ids)))
    new_re_users = tuple(new_re_users)
    print('New RE Users', new_re_users)

    if new_re_users:
        first_recommendation(new_re_users, config_prod)