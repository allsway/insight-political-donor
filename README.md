# insight-political-donor
Calculates total, median and number of donations based on zip code and date

#### Requirements
The following script requires python3, and expects the following modules to be installed:
- csv
- heapq
- heapq_max
- dateparser
- datetime
- decimal

As the first argument, the script users the header file available for contributions by indviduals, for future flexibility: http://classic.fec.gov/finance/disclosure/metadata/indiv_header_file.csv

The file should be downloaded and included in the input directory.  

The script can be run as `python ./src/calculate_donations.py ./input/{data_dict.csv} ./input/{input_data.txt} ./output/{output_zip_file_name.txt} ./output/{output_date_file_name.txt}` or it can be run with `./run.sh`

#### calculate_donations.py
Source code that reads in the political donor data, and produces the output 
- median_values_by_zip.txt 
- median_values_by_date.txt

In order to calculate the median for median_values_by_zip.txt, the CMTE_ID is stored in a dictionary, which contains a sub-dictionary for each zipcode that has been contributed by.  Each zip code hash contains a 4 column dictionary.  The transaction amounts are then stored in a min heap and a max heap per zip code, in order to calculate the streaming median most efficiently.  The 4 columns of the zip code hash contain:
 - min heap (the upper half of the transaction amounts per zip code per CMTE_ID streamed in so far).  If there are an odd number of transactions, the min heap should always contain the extra transaction.  
 - max heap (the lower half of the transaction amounts per zip code per CMTE_ID streamed in so far)
 - Running total of the number of transactions per zip code per CMTE_ID
 - Running sum of transactions per zip code per CMTE_ID streamed in so far
```
[CMTE_ID] : {
  [ZIP_CODE] : {min_heap: [], max_heap: [], transaction_count: 2, transaction_total: 1300}
  [ZIP_CODE] : {...}
  [ZIP_CODE] : {...}
  [ZIP_CODE] : {...}
  ...
},
 [CMTE_ID] : {
  ...
  }
```
In the data streaming case, we must store data for each CMTE_ID and zip code combination, and must maintain all transaction amount values in order to calculate the median.  

In order to calculate the median for median_values_by_date.txt, the data must be outputted in sorted order (by both CMTE_ID and transaction date).  The data must be sorted at least once in order to acheive this result. Because of this, I sort the incoming data by CMTE_ID and transaction date  prior to processing the data. As the data is in sorted order, and the output file should only have one line per CMTE_ID and transaction date combination, this data is only temporarily stored until the output line is computed for each CMTE_ID/transaction date combination (until the next CMTE_ID or transaction date is reached).  The transaction values are the only thing that need to be stored, and they are stored in an array:

```
  median_values = [250, 800, 320, 129, 31, 93, 99]
```

The transaction count and total are calculated from the resulting array of transaction amounts.  




