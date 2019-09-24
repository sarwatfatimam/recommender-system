#!~/anaconda3/bin/python

import pandas as pd
from dealsmash import *
import credentials
import variables
import psycopg2
from table_insertions import *

# Connecting to prod database using psycopg2
config_prod = credentials.config_prod
con_prod = psycopg2.connect(**config_prod)
cursor_prod = con_prod.cursor()

if __name__ == "__main__":

    df_user = pd.read_sql('SELECT * FROM ds_user WHERE CAST(created_at as DATE) <= now()', con=con_prod)

    # Filter out rows with null contacts
    df_null_contacts = df_user[pd.isnull(df_user['contact'])]
    df_user = df_user[~pd.isnull(df_user['contact'])]

    # Filter out users with invalid contact numbers
    df_invalid_contact = df_user.ix[(df_user['contact'].map(len) > 11)]
    df_user = df_user.ix[~(df_user['contact'].map(len) > 11)]

    # Filter out rows with name in test, Deal Smash, tessts, tests, testrelease610R
    test_accounts = variables.test_accounts
    df_test = df_user[df_user['first_name'].isin(test_accounts)]
    df_partial_test = df_user[df_user['first_name'].str.contains("test")]

    df_all_removed = pd.concat([df_null_contacts, df_test, df_partial_test, df_invalid_contact])
    df_all_removed = df_all_removed.drop_duplicates()
    print('The total number of removed users are', len(df_all_removed))
    df_all_removed = df_all_removed[['id']]

    print('Concatenating data for bulk insert')
    data = [tuple(row) for index, row in df_all_removed.iterrows()]
    data = tuple(data)

    cursor_prod.close()

    print('Dropping data from the table before bulk inserting since it is faster than bulk update or bulk upsert')
    delete_removed_accounts(config_prod)

    print('Bulk inserting removed accounts data')
    insert_removed_accounts(data, config_prod)


