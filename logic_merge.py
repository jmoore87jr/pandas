import pandas as pd
import s3fs
from sqlalchemy import create_engine
import psycopg2
from credentials import *

# TODO: have load_from_s3 take *args and return a list of dataframes of any length

""" 
* join DSAMS and CISIL data from S3 using Pandas
* DSAMS data is pipe delimited and the ID columns need to be combined to match the CISIL ID column
* CISIL data is fixed width
"""

PIPE_DELIMITED_FILES = ['s3://mnnk/pipe_data1.csv', 's3://mnnk/pipe_data2.csv', 's3://mnnk/pipe_data3.csv']
FIXED_WIDTH_FILE = 's3://mnnk/fw_data.csv'

def concat_dfs(csvs, sep=',', fwf=False):
    """ 
    * take a list of pandas dataframes and concatenate them vertically
    * sep is the delimiter
    * fwf indicates whether the file is fixed width as opposed to delimited
    """

    # turn the list of csvs into a list of Pandas dataframes
    dfs = [ pd.read_fwf(file) if fwf else pd.read_csv(file, sep=sep) for file in csvs ]

    # concatenate dataframes and reset index
    result = pd.concat(dfs, ignore_index=True)
    
    return result

def load_from_s3():
   # pipe delimited
    df1 = concat_dfs(PIPE_DELIMITED_FILES)

    # fixed width
    df2 = pd.read_fwf(FIXED_WIDTH_FILE)

    print("Loaded files from s3")

    return [df1,df2]


def merge_dfs(df1, df2):
    # concatenate the 3 separate ID rows in df1 and remove the partial ID columns
    df1.insert(0, 'ID', df1['ID1'].astype('str') + df1['ID2'] + df1['ID3'])
    df1 = df1[['ID', 'A', 'B']]

    # merge the dataframes on ID and sort by ID
    result = df1.merge(df2, left_on='ID', right_on='ID').sort_values(by='ID')

    print("Merged dataframes")
    print(result)

    return result

def save_to_rds(df):
    try:
        # connect to database
        conn = psycopg2.connect(
            host=DATABASE_ENDPOINT,
            database=DATABASE_NAME,
            user=USERNAME,
            password=PASSWORD,
            port=PORT
        )
        print("Connected to database")

        # create engine
        engine = create_engine(f'postgresql://{USERNAME}:{PASSWORD}@{DATABASE_ENDPOINT}:{PORT}/{DATABASE_NAME}')

        # insert data
        try:
            df.to_sql('table1', con=engine, if_exists='append', chunksize=20000)
        except:
            print("Data format doesn't match database format")

        # commit to database
        conn.commit()
        print("Saved to database")

        # close connection
        conn.close()
        print("Connection closed")

    except psycopg2.OperationalError as e:
        print(e)


if __name__ == "__main__":
    
    dfs = load_from_s3()

    df = merge_dfs(dfs[0], dfs[1])

    save_to_rds(df)