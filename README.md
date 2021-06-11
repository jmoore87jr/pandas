# Instructions

1. Make a credentials.py file that looks like this:

```
# S3 Credentials
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
REGION_NAME = ''

# RDS Credentials
USERNAME = ''
PASSWORD = ''
DATABASE_ENDPOINT = ''
PORT = ''
DATABASE_NAME = ''

# S3 Files
PIPE_DELIMITED_FILES = list_of_s3_files
FIXED_WIDTH_FILE = s3_file
```

2. Use merge_and_sort.ipynb to benchmark Pandas for data of different sizes.

3. chunks.py shows how to dump the data into a SQLite database in chunks to decrease memory usage.


# Pandas

We use `merge()` instead of the faster `join()` because we need to join on columns rather than indexes.

`merge()` and `sort()` take similar amounts of time; about 3.5-4 seconds for an 840MB DataFrame (10 million rows), and 39-40 seconds for an 8.2GB DataFrame (100 million rows).


Tips for using Pandas with large data:
* When using `read_csv()` specify `usecols=[<columns>]` to avoid loading the entire csv into memory
* Use `categorical (enumerate)` instead of `object (string)` data type when possible
* Downcast numeric columns using `pd.to_numeric()`
* Use chunking (note: if processing requires coordination between chunks, don't use Pandas)



