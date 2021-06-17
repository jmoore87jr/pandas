import pandas as pd 
from datetime import datetime

# create dataframe
df = pd.DataFrame({'ID': [1001, 1002, 1003, 1004],
    'Item': ['Hello', 'World', 'Foo', 'Bar'],
    'Date': ['12-OCT-06',
            '12-OCT-31',
            '12-NOV-20',
            '12-DEC-04'],
    'Date2': ['2012-10-06',
              '2012-10-31',
              '2012-11-20',
              '2012-12-04'],
    'Date3': ['10/06/2012',
              '10/31/2012',
              '11/20/2012',
              '12/04/2012']
            })

print("-----ORIGINAL DATAFRAME----- \n", df, "\n")

# identify date columns and convert
for col in df.columns:
    if df[col].dtype in ['object', 'datetime64']:
        try: # if the column isn't in our date format, we get a ValueError and skip it
            df[col] = df[col].apply(lambda _date: datetime.strptime(_date, '%y-%b-%d').strftime('%m-%d-%Y'))            
        except ValueError:
            pass
        try: 
            df[col] = df[col].apply(lambda _date: datetime.strptime(_date, '%Y-%m-%d').strftime('%m-%d-%Y'))            
        except ValueError:
            pass
        try: 
            df[col] = df[col].apply(lambda _date: datetime.strptime(_date, '%m/%d/%Y').strftime('%m-%d-%Y'))            
        except ValueError:
            pass

# for date codes, refer to https://strftime.org/

print("-----NEW DATAFRAME----- \n", df, "\n")

# Pandas can try to convert with...
#     df[col] = pd.to_datetime(df[col], format='%y-%b-%d')
# but I can't figure out how to convert to MM-DD-YYYY, so we use the datetime library
#     df[col] = df[col].apply(lambda _date: datetime.strptime(_date, '%y-%b-%d').strftime('%m-%d-%Y'))

