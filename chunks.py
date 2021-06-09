import pandas as pd 
import time 
from save_to_db import save_to_sqlite

# rows = 1 million
# chunks = 20000 -> 18.5s
# chunks = 50000 -> 17.9s
# chunks = 100000 -> 17.2s

# 10 million rows, 25k chunks -> 484.8s

# best approach for large-ish data (?): 
#  chunk into database, then sort from there rather than in memory with Python

def chunk_and_save(rows):
    """ read in 2 datasets in chunks and save them to sqlite """

    i = 1

    for chunk1, chunk2 in zip(pd.read_csv(f'data/heights_{rows}.csv', chunksize=25000, header=0), 
                            pd.read_csv(f'data/numbers_{rows}.csv', chunksize=25000, header=0)): 
        
        df = chunk1.merge(chunk2, left_on='ID', right_on='ID')

        save_to_sqlite(df)

        print(f"Chunk {i} processed")

        i += 1


if __name__ == "__main__":
    start_time = time.time()

    chunk_and_save(10000000)

    print(f"Program took {(time.time() - start_time)} seconds")