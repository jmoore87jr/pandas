import pandas as pd 

# TODO: retreive the data from S3, and write it out to RDS postgres database

""" 
* boilerplate for joining DSAMS and CISIL data using Pandas
* DSAMS data is pipe delimited and the ID columns need to be combined to match the CISIL ID column
* CISIL data is fixed width
"""

# read pipe-delimited data
# NOTE: include "header=<n>" in the read_csv call to deal with headers
df1 = pd.read_csv('data/pipe_data.csv', sep='|')

# read fixed width data
df2 = pd.read_fwf('data/fw_data.csv')

print(df1.head(3))
print(df2.head(3))

# concatenate the 3 separate ID rows in df1 and remove the partial ID columns
df1.insert(0, 'ID', df1['ID1'].astype('str') + df1['ID2'] + df1['ID3'])
df1 = df1[['ID', 'A', 'B']]

# merge the dataframes on ID and sort by ID
result = df1.merge(df2, left_on='ID', right_on='ID').sort_values(by='ID')

print(result)