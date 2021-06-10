import pandas as pd
import credentials
import s3fs

# TODO: have load_from_s3 take *args and return a list of dataframes of any length

""" 
* join DSAMS and CISIL data from S3 using Pandas
* DSAMS data is pipe delimited and the ID columns need to be combined to match the CISIL ID column
* CISIL data is fixed width
"""

def load_from_s3():
    
   # pipe delimited
    df1 = pd.read_csv('s3://mnnk/pipe_data.csv', sep='|')

    # fixed width
    df2 = pd.read_fwf('s3://mnnk/fw_data.csv')

    return [df1,df2]


def merge_dfs(df1, df2):

    # concatenate the 3 separate ID rows in df1 and remove the partial ID columns
    df1.insert(0, 'ID', df1['ID1'].astype('str') + df1['ID2'] + df1['ID3'])
    df1 = df1[['ID', 'A', 'B']]

    # merge the dataframes on ID and sort by ID
    result = df1.merge(df2, left_on='ID', right_on='ID').sort_values(by='ID')

    print(result)

    return result


if __name__ == "__main__":
    dfs = load_from_s3()

    merge_dfs(dfs[0], dfs[1])