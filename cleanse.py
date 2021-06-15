import pandas as pd

"""
Alter the load_from_s3() function in main.py to read column widths and names
"""

colnames = ['col1', 'col2', 'col3']

# read in the fixed width file
# 'widths' takes a list of column widths
# 'names' takes a list of column names
df = pd.read_fwf('data/L02.TEST3.FILE00.NAME', widths=[5, 15, 12], names=colnames)

# make one of the values 'None'
df.iloc[1,2] = None

print(df)

# return rows with missing values
null_data = df[df.isnull().any(axis=1)]

print(null_data)
