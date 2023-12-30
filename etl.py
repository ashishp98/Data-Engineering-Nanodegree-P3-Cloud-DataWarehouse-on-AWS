import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    """
    Loads data from the S3 to the staging table on Redshift
    """
    
    print("Loading data from JSON files stored in S3 buckets into staging tables")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print("Complete.\n")


def insert_tables(cur, conn):
    
    """
    Inserts data from the staging table to the facts and dimension tables of the Datawarehouse, created on Redshift
    """
    
    print("Inserting data from staging tables into Redshift tables")
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    print("Complete.\n")


def main():
    
    """
    - Establishes connection with the Redshift cluster and gets cursor to it.  
    - Loads data from the S3 to the staging table on Redshift
    - Inserts data from staging tables into Redshift tables
    - Finally, closes the connection. 
    """
        
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()