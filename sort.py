import pandas as pd 
import time

# sorting 10 million rows takes ~3s
# 100 million takes 18.5s

def sort(numrows, maxrows):
    """ sorts a dataframe and times it """

    while numrows <= maxrows:
        # load the dataframes
        df = pd.read_csv(f'data/heights_{numrows}.csv')

        print(f"Sorting {numrows} rows...")

        # time the merge
        start_time = time.time()
        df = df.sort_values(by='Height', ascending=False)
        print(f"Sort took {(time.time() - start_time)} seconds")

        numrows *= 10

        print("Sleeping for 3s...")
        time.sleep(3)


if __name__ == "__main__":
    numrows = 10
    maxrows = 100000000 # 100 million

    sort(numrows, maxrows)