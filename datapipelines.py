#!~/anaconda3/bin/python

import timeit
import psutil
import os


start = timeit.default_timer()

print('Extracting current registered users on dealsmash')
with open('/home/ec2-user/extract_registered_users.py') as source_file:
    exec(source_file.read())

stop = timeit.default_timer()
time = stop - start
print('It took ', time, ' to run this script')

pid = os.getpid()
py = psutil.Process(pid)
memoryUse = py.memory_info()[0]/2.**30
print('memory use:', memoryUse)

start = timeit.default_timer()

print('Extracting coupons saved for implicit ratings')
with open('/home/ec2-user/implicit_coupon_saved.py') as source_file:
    exec(source_file.read())

stop = timeit.default_timer()
time = stop - start
print('It took ', time, ' to run this script')

pid = os.getpid()
py = psutil.Process(pid)
memoryUse = py.memory_info()[0]/2.**30
print('memory use:', memoryUse)

start = timeit.default_timer()

print('Extracting coupons unsaved for implicit ratings')
with open('/home/ec2-user/implicit_coupon_unsaved.py') as source_file:
    exec(source_file.read())

stop = timeit.default_timer()
time = stop - start
print('It took ', time, ' to run this script')

pid = os.getpid()
py = psutil.Process(pid)
memoryUse = py.memory_info()[0]/2.**30
print('memory use:', memoryUse)

start = timeit.default_timer()

print('Extracting coupon views for implicit ratings')
with open('/home/ec2-user/implicit_coupon_views.py') as source_file:
    exec(source_file.read())

stop = timeit.default_timer()
time = stop - start
print('It took ', time, ' to run this script')

pid = os.getpid()
py = psutil.Process(pid)
memoryUse = py.memory_info()[0]/2.**30

start = timeit.default_timer()

print('Extracting store views for implicit ratings')
with open('/home/ec2-user/implicit_store_views.py') as source_file:
    exec(source_file.read())

stop = timeit.default_timer()
time = stop - start
print('It took ', time, ' to run this script')

pid = os.getpid()
py = psutil.Process(pid)
memoryUse = py.memory_info()[0]/2.**30
print('memory use:', memoryUse)

start = timeit.default_timer()

print('Extracting coupon explicit ratings')
with open('/home/ec2-user/explicit_ratings.py') as source_file:
    exec(source_file.read())

stop = timeit.default_timer()
time = stop - start
print('It took ', time, ' to run this script')

pid = os.getpid()
py = psutil.Process(pid)
memoryUse = py.memory_info()[0]/2.**30
print('memory use:', memoryUse)

start = timeit.default_timer()

print('Extracting Unified Ratings')
with open('/home/ec2-user/unified_ratings.py') as source_file:
    exec(source_file.read())

stop = timeit.default_timer()
time = stop - start
print('It took ', time, ' to run this script')

pid = os.getpid()
py = psutil.Process(pid)
memoryUse = py.memory_info()[0]/2.**30
print('memory use:', memoryUse)

start = timeit.default_timer()

print('Running Main Script')
with open('/home/ec2-user/Main_Script_CF.py') as source_file:
    exec(source_file.read())

stop = timeit.default_timer()
time = stop - start
print('It took ', time, ' to run this script')

pid = os.getpid()
py = psutil.Process(pid)
memoryUse = py.memory_info()[0]/2.**30
print('memory use:', memoryUse)

start = timeit.default_timer()

print('Filtering the extracted ratings')
with open('/home/ec2-user/re_cf_rating_filtered_fast.py') as source_file:
    exec(source_file.read())

stop = timeit.default_timer()
time = stop - start
print('It took ', time, ' to run this script')

pid = os.getpid()
py = psutil.Process(pid)
memoryUse = py.memory_info()[0]/2.**30
print('memory use:', memoryUse)