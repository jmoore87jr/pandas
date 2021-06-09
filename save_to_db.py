import pandas as pd 
from sqlalchemy import create_engine
import sqlite3


def save_to_sqlite(table):
    try:
        db = sqlite3.connect('database/dummy.db')
        cursor = db.cursor()
        engine = create_engine('sqlite:///database/dummy.db')
        table.to_sql('merged_table', con=engine, if_exists='append', chunksize=20000)
        db.commit()
        print(f"Added {table} to database")
    except IOError as e:
        print(e)
    finally:
        cursor.close()
        db.close()
