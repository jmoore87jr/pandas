import pandas as pd 
import numpy as np
import random


def generate(rows):
    """generate two data sets to test merge/join performance"""

    # random numbers with an ID column at position 0 to merge on
    df1 = pd.DataFrame(np.random.randint(0,100,size=(rows, 7))).reset_index()
    df1.columns = ['ID', 'A', 'B', 'C', 'D', 'E', 'F', 'G']

    # ID, height, weight
    df2 = pd.DataFrame(np.random.normal(67,5,size=(rows,2))).reset_index()
    df2.columns = ['ID', 'Height', 'Wingspan']

    print(f"dataframes generated with {rows} rows")

    df1.to_csv(f'data/numbers_{rows}.csv')
    df2.to_csv(f'data/heights_{rows}.csv')

    return [df1,df2]



