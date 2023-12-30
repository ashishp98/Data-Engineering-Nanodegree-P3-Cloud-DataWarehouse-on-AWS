import psycopg2
import configparser


def analyze_query(cur, conn):
    
    """
    Run the queries to count the number of rows present in each  staging and dimensional tables.
    """
    
    analyze_queries = [
    'SELECT COUNT(*) AS total FROM staging_events_table',
    'SELECT COUNT(*) AS total FROM staging_songs_table'
    'SELECT COUNT(*) AS total FROM song_table',
    'SELECT COUNT(*) AS total FROM songplay_table',
    'SELECT COUNT(*) AS total FROM artist_table',
    'SELECT COUNT(*) AS total FROM user_table',
    'SELECT COUNT(*) AS total FROM time_table']
    
    for query in analyze_queries:
        print('Running -> ' + query)
        cur.execute(query)
        results = cur.fetchone()

        for row in results:
            print("   ", row)
            conn.commit()


def main():
    
    """
    - Establishes connection with the Redshift cluster and gets cursor to it.  
    - Run COUNT(*) query to count the numbers of rows available in the staging and dimensional tables 
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    analyze_query(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()