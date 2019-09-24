#!~/anaconda3/bin/python

import psycopg2
from psycopg2.extensions import adapt, register_adapter, AsIs
import numpy as np


def adapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


def delete_explicit_ratings(config):
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        # Drop rows from table
        cur.execute("DELETE FROM ONLY re_explicit_ratings")
        cur.execute("CREATE SEQUENCE IF NOT EXISTS explicit_id_seq MINVALUE 1")
        cur.execute("ALTER TABLE re_explicit_ratings "
                    "ALTER id SET DEFAULT nextval('explicit_id_seq')")
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            

def insert_explicit_ratings(data, config):
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        register_adapter(np.int64, adapt_numpy_int64)
        data_string = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)", x).decode('utf-8') for x in data)
        # execute the INSERT statement
        cur.execute("INSERT INTO re_explicit_ratings(user_id, coupon_id, category_id, retailer_id, "
                    "retailer_status, rating, standard_coupon, discount, discount_y) VALUES " + data_string)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def delete_coupon_redemptions(config):
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        # Drop rows from table
        cur.execute("DELETE FROM ONLY re_coupon_redemptions")
        cur.execute("CREATE SEQUENCE IF NOT EXISTS redemptions_id_seq MINVALUE 1")
        cur.execute("ALTER TABLE re_coupon_redemptions "
                    "ALTER id SET DEFAULT nextval('redemptions_id_seq')")
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_coupon_redemptions(data, config):
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        data_string = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)", x).decode('utf-8') for x in data)
        # execute the INSERT statement
        cur.execute("INSERT INTO re_coupon_redemptions(user_id, coupon_id, redemption_date, category_id, retailer_id, "
                    "retailer_status, standard_coupon, discount, discount_y) VALUES " + data_string)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def delete_coupon_saved(config):
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        # Drop rows from table
        cur.execute("DELETE FROM ONLY re_coupon_saved")
        cur.execute("CREATE SEQUENCE IF NOT EXISTS saved_id_seq MINVALUE 1")
        cur.execute("ALTER TABLE re_coupon_saved "
                    "ALTER id SET DEFAULT nextval('saved_id_seq')")
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_coupon_saved(data, config):
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        data_string = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)", x).decode('utf-8') for x in data)
        # execute the INSERT statement
        cur.execute("INSERT INTO re_coupon_saved(user_id, coupon_id, time, category_id, retailer_id, "
                    "retailer_status, standard_coupon, discount, discount_y) VALUES " + data_string)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def delete_coupon_unsaved(config):
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        # Drop rows from table
        cur.execute("DELETE FROM ONLY re_coupon_unsaved")
        cur.execute("CREATE SEQUENCE IF NOT EXISTS unsaved_id_seq MINVALUE 1")
        cur.execute("ALTER TABLE re_coupon_unsaved "
                    "ALTER id SET DEFAULT nextval('unsaved_id_seq')")
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_coupon_unsaved(data, config):
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        data_string = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)", x).decode('utf-8') for x in data)
        # execute the INSERT statement
        cur.execute("INSERT INTO re_coupon_unsaved(user_id, coupon_id, time, category_id, retailer_id, "
                    "retailer_status, standard_coupon, discount, discount_y) VALUES " + data_string)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def delete_coupon_views(config):
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        # Drop rows from table
        cur.execute("DELETE FROM ONLY re_coupon_views")
        cur.execute("CREATE SEQUENCE IF NOT EXISTS views_id_seq MINVALUE 1")
        cur.execute("ALTER TABLE re_coupon_views "
                    "ALTER id SET DEFAULT nextval('views_id_seq')")
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_coupon_views(data, config):
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        register_adapter(np.int64, adapt_numpy_int64)
        data_string = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)", x).decode('utf-8') for x in data)
        # execute the INSERT statement
        cur.execute("INSERT INTO re_coupon_views(user_id, coupon_id, time, category_id, retailer_id, "
                    "retailer_status, standard_coupon, discount, discount_y) VALUES " + data_string)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def delete_store_views(config):
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        # Drop rows from table
        cur.execute("DELETE FROM ONLY re_store_views")
        cur.execute("CREATE SEQUENCE IF NOT EXISTS store_id_seq MINVALUE 1")
        cur.execute("ALTER TABLE re_store_views "
                    "ALTER id SET DEFAULT nextval('store_id_seq')")
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_store_views(data, config):
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        register_adapter(np.int64, adapt_numpy_int64)
        data_string = ','.join(cur.mogrify("(%s,%s,%s,%s)", x).decode('utf-8') for x in data)
        # execute the INSERT statement
        cur.execute("INSERT INTO re_store_views(user_id, retailer_id, retailer_status, "
                    "time) VALUES " + data_string)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def delete_removed_accounts(config):
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        # Drop rows from table
        cur.execute("DELETE FROM ONLY re_removed_accounts")
        cur.execute("CREATE SEQUENCE IF NOT EXISTS removed_id_seq MINVALUE 1")
        cur.execute("ALTER TABLE re_removed_accounts "
                    "ALTER id SET DEFAULT nextval('removed_id_seq')")
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_removed_accounts(data, config):
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        register_adapter(np.int64, adapt_numpy_int64)
        data_string = ','.join(cur.mogrify("(%s)", x).decode('utf-8') for x in data)
        # execute the INSERT statement
        cur.execute("INSERT INTO re_removed_accounts(id) VALUES " + data_string)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            

def delete_cf_ratings(config):
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        # Drop rows from table
        cur.execute("DELETE FROM ONLY re_cf_rating")
        cur.execute("CREATE SEQUENCE IF NOT EXISTS cf_id_seq MINVALUE 1")
        cur.execute("ALTER TABLE re_cf_rating "
                    "ALTER id SET DEFAULT nextval('cf_id_seq')")    
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def delete_cf_ratings_master(config):
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        # Drop rows from table
        cur.execute("DELETE FROM ONLY re_cf_rating_master")
        cur.execute("CREATE SEQUENCE IF NOT EXISTS mcf_id_seq MINVALUE 1")
        cur.execute("ALTER TABLE re_cf_rating_master "
                    "ALTER id SET DEFAULT nextval('mcf_id_seq')")
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_cf_ratings(data, config):
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        register_adapter(np.int64, adapt_numpy_int64)
        data_string = ','.join(cur.mogrify("(%s,%s,%s,%s)", x).decode('utf-8') for x in data)
        # execute the INSERT statement
        cur.execute("INSERT INTO re_cf_rating(user_id, item_id, ratings, category_id) VALUES " + data_string)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_cf_ratings_master(data, config):
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        # register_adapter(np.int64, adapt_numpy_int64)
        data_string = ','.join(cur.mogrify("(%s,%s,%s)", x).decode('utf-8') for x in data)
        # execute the INSERT statement
        cur.execute("INSERT INTO re_cf_rating_master(user_id, item_id, ratings) VALUES " + data_string)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def delete_unified_ratings(config):
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        # Drop rows from table
        cur.execute("DELETE FROM ONLY re_unified_ratings")
        cur.execute("CREATE SEQUENCE IF NOT EXISTS unified_id_seq MINVALUE 1")
        cur.execute("ALTER TABLE re_unified_ratings "
                    "ALTER id SET DEFAULT nextval('unified_id_seq')")
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_unified_ratings(data, config):
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        register_adapter(np.int64, adapt_numpy_int64)
        data_string = ','.join(cur.mogrify("(%s,%s,%s)", x).decode('utf-8') for x in data)
        # execute the INSERT statement
        cur.execute("INSERT INTO re_unified_ratings(user_id, coupon_id, rating) VALUES " + data_string)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def update_user(data, config):  
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        cur.execute("UPDATE ds_user SET has_recommendations = NULL, recommendation_sent = NULL")
        # execute the INSERT statement
        cur.execute("UPDATE ds_user SET has_recommendations = TRUE WHERE id IN " + str(data))
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()       


def first_recommendation(data, config):
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        cur.execute("UPDATE ds_user SET first_recommendation = DATE_TRUNC('second', NOW()) WHERE "
                    "id IN " + str(data))
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def delete_control_group(data, config):
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**config)
        # create a new cursor
        cur = conn.cursor()
        # Drop rows from table
        cur.execute("DELETE FROM ONLY re_cf_rating WHERE user_id IN " + str(data))
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()