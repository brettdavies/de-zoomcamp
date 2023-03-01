## Week 3 Homework

## Question 1:
What is the count for fhv vehicle records for year 2019?
- 43,244,696


## Question 2:
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
- 0 MB for the External Table and 317.94MB for the BQ Table 


## Question 3:
How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
- 717,748


## Question 4:
What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?
- Partition by pickup_datetime Cluster on affiliated_base_number


## Question 5:
What are these values? Choose the answer which most closely matches.
- 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table


## Question 6: 
Where is the data stored in the External Table you created?
- GCP Bucket


## Question 7:
It is best practice in Big Query to always cluster your data:
- False


## (Not required) Question 8:
A better format to store these files may be parquet. Create a data pipeline to download the gzip files and convert them into parquet. Upload the files to your GCP Bucket and create an External and BQ Table. 

DONE!