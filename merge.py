import pandas as pd 
import time
from generate import generate


def run(numrows, maxrows):
    while numrows <= maxrows:
        # generate the dataframes
        print("Generating data...")
        df1, df2 = generate(numrows)

        # merge
        print(f"Merging {numrows} rows...")
        start_time = time.time()
        df = df1.merge(df2, left_on="ID", right_on="ID")
        print(f"Merge took {(time.time() - start_time)} seconds")

        # sort
        print(f"Sorting {numrows} rows...")
        start_time = time.time()
        df = df.sort_values(by='Height', ascending=False)
        print(f"Sort took {(time.time() - start_time)} seconds")

        # memory usage of final df
        print(df.info(memory_usage="deep"))

        numrows *= 10

        print("Sleeping for 3s...")
        time.sleep(3)


if __name__ == "__main__":
    numrows = 10
    maxrows = 100000000 # 100 million

    run(numrows, maxrows)



