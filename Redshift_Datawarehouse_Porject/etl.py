import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description:
    This function is responsible for load data to staging tables.

    Args:
        conn: psycopg2 connection
        cur: psycopg2 cursor
    
    Returns:
        None
    """
    for query in copy_table_queries:
        try: 
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(e)

def insert_tables(cur, conn):
    """
    Description:
    This function is responsible for insert data to fact and dimension table.

    Args:
        None
    
    Returns:
        None
    """
    for query in insert_table_queries:
        try: 
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(e)


def main():
    """
    Description:
    This main function is responsible connect AWS redshift cluster 
    and execute load_staging_tables and insert_tables function.

    Args:
        conn: psycopg2 connection
        cur: psycopg2 cursor
    
    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
    except psycopg2.Error as e:
        print(e)
    
    try: 
        load_staging_tables(cur, conn)
        print("The staging tables has been loaded.")
    except Exception as e:
        print(e)
    
    try:
        insert_tables(cur, conn)
        print("The fact and dimension table has been inserted")
    except Exception as e:
        print(e)

    conn.close()


if __name__ == "__main__":
    main()