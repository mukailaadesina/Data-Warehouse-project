import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Creates each table using the queries in 'copy_table_queries' list. 
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Creates each table using the queries in 'insert_table_queries' list. 
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Reads dwh.cfg in the project root folder. 
    Open connection to the cluster
    Crete cursor
    Load data to staging tables
    Insert data into data warehouse tables
    Close the connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('1. Connected')
    cur = conn.cursor()
    print('2. Created Cursor')
    
    load_staging_tables(cur, conn)
    print('3. Load data to staging')
    insert_tables(cur, conn)
    print('4. Insert data into tables')

    conn.close()
    print('5. Close connection')


if __name__ == "__main__":
    main()