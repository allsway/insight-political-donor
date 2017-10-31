#!/usr/bin/env python3
import sys
import csv
import re
from heapq_max import *
from heapq import *
import dateparser
import datetime
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
    if isinstance(date, datetime.datetime):
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

# Set output data
def format_output(data_store,cmte_id,zip):
    median = get_median_zip(data_store[cmte_id][zip])
    median = round_median(median)
    output = [cmte_id, zip, median, data_store[cmte_id][zip]['transaction_count'], int(data_store[cmte_id][zip]['transaction_total'])]
    return output

# Outputs our data files
def write_data_to_file(output,filename):
    print (output)
    with open (filename, 'w') as f:
        w = csv.writer(f, delimiter='|')
        for row in output:
            w.writerow(row)

# Rounds 1.5 up to 2, and rounds 1.49 down to 1
def round_median(num):
    return Decimal(num).quantize(Decimal(0), rounding=ROUND_HALF_UP)

# Returns regular median from a sorted array (our median is sorted here, because the input data is sorted)
def get_median_date(median_values):
    median_values = sorted(median_values, key=float)
    # if array is odd, return middle element
    if len(median_values) % 2 != 0:
        median_index = ((len(median_values) + 1) / 2) - 1
        return median_values[int(median_index)]
    # Otherwise, if the array is even, return the average of the middle two elements
    else:
        mid = len(median_values)
        median = (int(median_values[int(mid / 2)]) + int(median_values[int(mid / 2) - 1])) / 2.0
        return median

# opens data input and calls functions for our zip output and date output
def open_data(input_file,index,zip_out_file,date_out_file):
    f = open(input_file, 'rt')
    try:
        reader = csv.reader(f, delimiter='|')
        zip_output = read_data_by_zip(reader,index)
        date_output = read_data_by_date(input_file,index)
        write_data_to_file(zip_output,zip_out_file)
        write_data_to_file(date_output,date_out_file)
    finally:
        f.close()


# Parse the date by transaction date, in order
def read_data_by_date(input_file,index):
    median_values, output_data = [], []
    f  = open(input_file, 'r')
    reader = csv.reader(f, delimiter='|')

    all_data = list(reader)
    cmte_id, transaction_amt, transaction_date = index['CMTE_ID'], index['TRANSACTION_AMT'], index['TRANSACTION_DT']
    all_data = sorted(all_data, key = lambda x: (x[cmte_id], x[transaction_date] ))

    # set our 'current' date and CMTE_ID
    old_cmte_id, old_date = all_data[0][cmte_id], all_data[0][transaction_date]
    for row in all_data:
        switch = False
        #print (row[0], row[13], row[14])

        if not row[index['OTHER_ID']] and row[cmte_id] and is_valid(row[transaction_date]):
            # keep going until we hit the next CMTE_ID
            cur_cmte_id,cur_transaction_date = row[cmte_id], row[transaction_date]
            # at switch, calculate and output the previous CMTE_ID/date combination data
            if cur_cmte_id != old_cmte_id :
                switch = True
                # add our current values to output array
                output_data.append([old_cmte_id, old_date, int(get_median_date(median_values)), len(median_values), int(sum(map(float,median_values))) ])
                # reset our data_by_date data
                median_values = []
                old_cmte_id = cur_cmte_id
                old_date = cur_transaction_date
            if cur_transaction_date != old_date and not switch:
                output_data.append([old_cmte_id, old_date, int(get_median_date(median_values)), len(median_values), int(sum(map(float,median_values)))])
                median_values = []
                old_date = cur_transaction_date

            # We only need to store the transaction amounts, and will calculate the rest based on this array
            median_values.append(row[transaction_amt])
    median = round_median(get_median_date(median_values))
    # because we are only processing the previous row, must still process the final cmte_id/date combo
    output_data.append([old_cmte_id,old_date, median,len(median_values),int(sum(map(float,median_values))) ])
    return output_data

# Reads zip data and calculates all output for the zip_data file
def read_data_by_zip(reader,index):
    row_num, output_data, data_store =  0, [], {}
    for row in reader:
        # continue only if OTHER_ID is an empty field, and TRANSACTION_AMT and CMTE_ID both have data
        zipcode, cmte_id, transaction_amt = reduce_zip(row[index['ZIP_CODE']]),  row[index['CMTE_ID']], row[index['TRANSACTION_AMT']]
        if not row[index['OTHER_ID']] and transaction_amt and cmte_id:
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
    return output_data


# reads in arguments: header file and data file
data_dict = sys.argv[1]
input_data = sys.argv[2]
out_file_zip = sys.argv[3]
out_file_date = sys.argv[4]

index = get_index(data_dict)
open_data(input_data,index,out_file_zip,out_file_date)
