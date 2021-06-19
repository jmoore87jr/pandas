import pandas as pd
import s3fs
from sqlalchemy import create_engine, text
import psycopg2
from credentials import *

# TODO: I think my table from pd.to_sql is not the same format as the one being upserted
#  1. try to CREATE TABLE in SQL instead of using pd.to_sql
#  2. ***I think the ID column has to be type UNIQUE to use ON CONFLICT

def generate_dummy_dfs():
    df1 = pd.DataFrame({'ID': [1001, 1002, 1003, 1004],
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
    
    df2 = pd.DataFrame({'ID': [1001, 1005],
    'Item': ['I', 'Widget'],
    'Date': ['REPLACED',
            '12-JAN-02'],
    'Date2': ['THIS',
              '2012-01-02'],
    'Date3': ['ROW',
              '01/02/2012']
            })
    return [df1, df2]

def connect_to_db():
    try:
        # connect to database
        conn = psycopg2.connect(
            host=UPSERT_DATABASE_ENDPOINT,
            database=UPSERT_DATABASE_NAME,
            user=UPSERT_USERNAME,
            password=UPSERT_PASSWORD,
            port=UPSERT_PORT
        )
        print("Connected to database")

        # create engine
        engine = create_engine(f'postgresql://{UPSERT_USERNAME}:{UPSERT_PASSWORD}@{UPSERT_DATABASE_ENDPOINT}:{UPSERT_PORT}/{UPSERT_DATABASE_NAME}')

    except psycopg2.OperationalError as e:
        print(e)
    
    return conn, engine


def create_table(df, table, conn, engine):

    engine.execute(f"DROP TABLE IF EXISTS {table}")

    sql = f"""
    CREATE TABLE {table} (
        "ID" INTEGER UNIQUE,
        "Item" VARCHAR,
        "Date" VARCHAR,
        "Date2" VARCHAR,
        "Date3" VARCHAR
    );
    """

    # execute upsert
    engine.execute(sql)



def upsert(df, table, conn, engine):
    
    # upsert Postgres database from Pandas dataframe
    sql = text(f""" 
            INSERT INTO {table} ("ID", "Item", "Date", "Date2", "Date3")
            VALUES {','.join([str(i) for i in list(df.to_records(index=False))])}
            ON CONFLICT ("ID")
            DO  
                UPDATE SET "ID" = EXCLUDED."ID",
                           "Item" = EXCLUDED."Item",
                           "Date" = EXCLUDED."Date",
                           "Date2" = EXCLUDED."Date2",
                           "Date3" = EXCLUDED."Date3";

        """)

    # execute upsert
    engine.execute(sql)


def view_table(conn, engine, table):

    # execute QUERY
    df = pd.read_sql_query(f'SELECT * FROM "{table}"', con=engine)

    print(f"-----Current Table----- \n {df}")


if __name__ == "__main__":
    dfs = generate_dummy_dfs()

    conn, engine = connect_to_db()

    create_table(dfs[0], 'table2', conn, engine)

    upsert(dfs[0], 'table2', conn, engine)

    view_table(conn, engine, 'table2')

    upsert(dfs[1], 'table2', conn, engine)

    view_table(conn, engine, 'table2')

    conn.commit()
    print("Changes committed")
    conn.close()
    print("Connection closed")
