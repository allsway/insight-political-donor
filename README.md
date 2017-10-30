# insight-political-donor
Calculates total, median and number of donations based on zip code and date

#### translate_data.py
Source code that reads in the political donor data, and produces the output 
- median_values_by_zip.txt 
- median_values_by_date.txt

In order to calculate the median for median_values_by_zip.txt, the CMTE_ID is stored in a dictionary, which contains a sub-dictionary for each zipcode that has been contributed by.  Each zip code hash contains a 4 column array.  The transaction amounts are then stored in a min heap and a max heap per zip code, in order to calculate the streaming median most efficiently.  The 4 columsn of the zip code hash contain:
 - min heap (the upper half of the transaction amounts per zip code per CMTE_ID streamed in so far)
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
For the running stream of data case, we must store data for each CMTE_ID and zip code combination, and must maintain each all transaction amount values in order to calculate the median.  

In order to calculate the median for median_values_by_date.txt, the data must be outputted in sorted order (by both CMTE_ID and transaction date).  The data must be sorted at least once in order to acheive this result.  


