#!~/anaconda3/bin/python

import pandas as pd
from dealsmash import *
import credentials
import psycopg2
from table_insertions import *
from surprise import Reader, SVD, dataset
from collections import defaultdict
import itertools

# Connecting to prod database using psycopg2
config_prod = credentials.config_prod
con_prod = psycopg2.connect(**config_prod)
cursor_prod = con_prod.cursor()


class CustomDataset(dataset.DatasetAutoFolds):
    def __init__(self, df, reader):
        self.raw_ratings = [(uid, iid, r, None) for (uid, iid, r) in
                            zip(df['user_id'], df['coupon_id'], df['rating'])]
        self.reader = reader


def get_top_n(predictions, n):

    # First map the predictions to each user.
    top_n = defaultdict(list)
    for uid, iid, true_r, est, _ in predictions:
        top_n[uid].append((iid, est))

    # Then sort the predictions for each user and retrieve the k highest ones.
    for uid, user_ratings in top_n.items():
        user_ratings.sort(key=lambda x: x[1], reverse=True)
        # top_n[uid] = user_ratings[:n]
        top_n[uid] = user_ratings[:]

    return top_n


if __name__ == "__main__":

    df = pd.read_sql("SELECT user_id, coupon_id, rating FROM re_unified_ratings", con=con_prod)
    reader = Reader(line_format='user item rating timestamp', sep=',')

    # How the data is read from the reader?
    data = CustomDataset(df, reader)

    # Running two algorithms
    algo = SVD()

    # Retrieve the trainset
    trainset = data.build_full_trainset()

    # Training dataset
    algo.train(trainset)

    testset = trainset.build_anti_testset()
    predictions = algo.test(testset)

    top_n = get_top_n(predictions, n=50)

    data = []
    user = []

    # Print the recommended items for each user
    for uid, user_ratings in top_n.items():
        d = [(uid, iid, float("{0:.3f}".format(est))) for (iid, est) in user_ratings]
        u = [(uid)]
        data.append(d)
        user.append(u)

    data = list(itertools.chain(*data))
    user = list(itertools.chain(*user))
    print("Concatenating rows for one single insertion")
    data = tuple(data)
    user = tuple(user)

    print('Update has_recommendations column of ds_user')
    update_user(user, config_prod)

    print('Dropping data from the table before bulk inserting since it is faster than bulk update or bulk upsert')
    delete_cf_ratings_master(config_prod)

    cursor_prod.close()

    print('Total length of the data', len(data))

    start_index = 0
    end_index = start_index + 500000
    length = round(len(data)/500000)
    for i in range(0, length + 1):
        new_data = data[start_index:end_index]

        if new_data:
           insert_cf_ratings_master(new_data, config_prod)

        start_index = end_index
        end_index = start_index + 500000
        if end_index > len(data):
            end_index = len(data) + 1
































