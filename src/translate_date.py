#!/usr/bin/env python3
import sys
import csv
import re
from heapq_max import *
from heapq import *
import dateparser
from decimal import Decimal, ROUND_HALF_UP, ROUND_HALF_DOWN

# Returns the column names mapped to their position in the file
def get_index(data_dict):
    f  = open(data_dict,'rt')
    try:
        reader = csv.reader(f)
        columns = next(reader)
        index = {}
        i = 0
        for col in columns:
            index[col] = i
            i = i + 1
    finally:
        f.close()
    return index

# Check if date is malformed
def is_valid(date):
    if date is None or date == '':
        return False
    date = dateparser.parse(date)
    print (type(date))
    if type(date) == 'datetime.datetime':
        return True
    else:
        return False

# Remove any non-numeral from the zip code, and return first 5 numbers
def reduce_zip(zip):
    zip = re.sub('[^0-9]','',zip)
    return zip[:5]

# Adds entries to the min half and max half transaction amounts by zip code
# min heap contains the top half, so [0] will give us the bottom element
# max heap contains the bottom half of the transactions, so that [0] will give us the top element
def add_to_heap(zip_data,new_entry):
    new_entry = float(new_entry)
    if not zip_data['lower_heap'] or new_entry > zip_data['lower_heap'][0]:
        heappush(zip_data['upper_heap'],new_entry)
        if len(zip_data['upper_heap']) > len(zip_data['lower_heap']) + 1:
            heappush_max(zip_data['lower_heap'], heappop(zip_data['upper_heap']))
    else:
        heappush_max(zip_data['lower_heap'], new_entry)
        if len(zip_data['lower_heap']) > len(zip_data['upper_heap']):
            heappush(zip_data['upper_heap'], heappop_max(zip_data['lower_heap']))
    return zip_data

# returns the median transaction value for a zip code
def get_median_zip(zip_data):
    if zip_data['lower_heap'] is None:
        return zip_data['upper_heap'][0]
    elif (len(zip_data['lower_heap']) + len(zip_data['upper_heap'])) % 2 != 0:
        return zip_data['upper_heap'][0]
    else:
        return ((zip_data['lower_heap'][0] + zip_data['upper_heap'][0]) / 2.0)

# Initialize data by zip code
def initialize_zip():
    heap = []
    heap_max = []
    data_by_zip = {
        'upper_heap': heap, # min heap
        'lower_heap': heap_max, # max heap
        'transaction_count': 0,
        'transaction_total': 0
    }
    return data_by_zip

# Initialize data by date
def initialize_date():
    data_by_date = {
        'median_values' : [],
        'transaction_count' : 0,
        'transaction_total' : 0
    }
    return data_by_date

# Set output data
def format_output(data_store,cmte_id,zip):
    # round zip
    median = get_median_zip(data_store[cmte_id][zip])
    median = round_median(median)
    output = [cmte_id, zip, median, data_store[cmte_id][zip]['transaction_count'], int(data_store[cmte_id][zip]['transaction_total'])]
#    print (oploutput)
    return output

# Outputs our data files
def write_data_to_file(output):
    print (output)
    with open ('medianvals_by_zip.txt', 'w') as f:
        w = csv.writer(f, delimiter='|')
        for row in output:
            w.writerow(row)

# Outputs our data files
def write_dates_to_file(output):
    print (output)
    with open ('medianvals_by_date.txt', 'w') as f:
        w = csv.writer(f, delimiter='|')
        for row in output:
            w.writerow(row)

# Rounds 1.5 up to 2, and rounds 1.49 down to 1
def round_median(num):
    return Decimal(num).quantize(Decimal(0), rounding=ROUND_HALF_UP)

# Returns regular median from a sorted array (our median is sorted here, because the input data is sorted)
def get_median_date(data_by_date):
    data_by_date['median_values'] = sorted(data_by_date['median_values'], key=float)
    print (data_by_date)
    # if array is odd, return middle element
    if data_by_date['transaction_count'] % 2 != 0:
        median_index = ((len(data_by_date['median_values']) + 1) / 2) - 1
        return data_by_date['median_values'][int(median_index)]
    # Otherwise, if the array is even, return the average of the middle two elements
    else:
        mid = len(data_by_date['median_values'])
        median = (int(data_by_date['median_values'][int(mid / 2)]) + int(data_by_date['median_values'][int(mid / 2) - 1])) / 2.0
        return median

# opens data input and calls functions for our zip output and date output
def open_data(input_file,index):
    f = open(input_file, 'rt')
    try:
        reader = csv.reader(f, delimiter='|')
        read_data_by_zip(reader,index)
        read_data_by_date(input_file,index)
    finally:
        f.close()


def switch_to_new_line():
    print ('yo')

# Parse the date by transaction date, in order
def read_data_by_date(input_file,index):
    data_by_date, output_data = {}, []
    f  = open(input_file, 'r')
    reader = csv.reader(f, delimiter='|')

    all_data = list(reader)
    all_data = sorted(all_data, key = lambda x: (x[index['CMTE_ID']], x[index['TRANSACTION_DT']] ))

    # set our 'current' date and CMTE_ID
    old_cmte_id, old_date = all_data[0][index['CMTE_ID']], all_data[0][index['TRANSACTION_DT']]
    data_by_date = initialize_date()

    for row in all_data:
        switch = False
        output_row = []
        #print (row[0], row[13], row[14])
        if not row[index['OTHER_ID']] and row[index['CMTE_ID']] and row[index['TRANSACTION_AMT']]: #and is_valid(row[index['TRANSACTION_DT']]):
            # keep going until we hit the next CMTE_ID
            cur_cmte_id,cur_transaction_date = row[index['CMTE_ID']], row[index['TRANSACTION_DT']]
            # at switch, calculate and output the previous CMTE_ID/date combination data
            if cur_cmte_id != old_cmte_id :
                switch = True
                print (data_by_date)
                # add our current values to output array
                output_row = [old_cmte_id, old_date, int(get_median_date(data_by_date)), data_by_date['transaction_count'], int(data_by_date['transaction_total'])]
                output_data.append(output_row)
                # reset our data_by_date data
                data_by_date = initialize_date()
                old_cmte_id = cur_cmte_id
                old_date = cur_transaction_date
                print (data_by_date)
            if cur_transaction_date != old_date and not switch:
                output_row = [old_cmte_id, old_date, int(get_median_date(data_by_date)), data_by_date['transaction_count'], int(data_by_date['transaction_total'])]
                output_data.append(output_row)
                data_by_date = initialize_date()
                old_date = cur_transaction_date

            # Now we can simply increment values stored in single array
            data_by_date['median_values'].append(row[index['TRANSACTION_AMT']])
            data_by_date['transaction_count'] += 1
            data_by_date['transaction_total'] += float(row[index['TRANSACTION_AMT']])
    print (output_data)
    median = round_median(get_median_date(data_by_date))
    output_data.append([old_cmte_id,old_date, median,data_by_date['transaction_count'],int(data_by_date['transaction_total']) ])
    # because we are only processing the previous row, must still process the final cmte_id/date combo
    #output_row = [old_cmte_id, old_date, int(get_median_date(data_by_date), data_by_date['transaction_count'], int(data_by_date['transaction_total']) ]
    #output_data.append(final_output_row)
    write_dates_to_file(output_data)


# Fills in the zipcode dict for each CMTE_ID
"""def fill_in_zip(zip_data,zip,transaction_amt):
    # update min and max heaps for zipcode by cmte_id
    zip_data = add_to_heap(zip_data,zip,transaction_amt)
    print (zip_data)
    # update transaction total
    zip_data[zip]['transaction_total'] = zip_data[zip]['transaction_total'] + float(transaction_amt)
    print (zip_data)
    # increment transaction count by 1
    zip_data[zip]['transaction_count'] = zip_data[zip]['transaction_count'] + 1
    return zip_data"""

# Reads zip data and calculates all output for the zip_data file
def read_data_by_zip(reader,index):
    row_num, output_data, data_store =  0, [], {}
    for row in reader:
        # continue only if OTHER_ID is an empty field, and TRANSACTION_AMT and CMTE_ID both have data
        zipcode, cmte_id, transaction_amt = reduce_zip(row[index['ZIP_CODE']]),  row[index['CMTE_ID']], row[index['TRANSACTION_AMT']]
        if not row[index['OTHER_ID']] and transaction_amt and cmte_id:
            print (row)
            # if this is the first time we encounter a cmte_id, add it to our large hash
            if cmte_id not in data_store:
                data_store[cmte_id] = {}
            if zipcode is not None and len(zipcode) > 4:
                # If zipcode has not yet been seen for cmte_id, initialize zipcode dict
                if zipcode not in data_store[cmte_id]:
                    data_store[cmte_id][zipcode] = initialize_zip()

                data_store[cmte_id][zipcode] = add_to_heap(data_store[cmte_id][zipcode],transaction_amt)
                # update transaction total
                data_store[cmte_id][zipcode]['transaction_total'] += float(transaction_amt)
                # increment transaction count by 1
                data_store[cmte_id][zipcode]['transaction_count'] += 1
                #fill_in_zip(data_store[cmte_id],zipcode,transaction_amt)
                print(get_median_zip(data_store[cmte_id][zipcode]))
                output_data.append( format_output(data_store,cmte_id,zipcode) )
    write_data_to_file(output_data)


# reads in arguments: header file and data file
data_dict = sys.argv[1]
input_data = sys.argv[2]

index = get_index(data_dict)
#read_data(input_data,index)
open_data(input_data,index)
