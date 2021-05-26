import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# define drop table function
def drop_tables (conn, cur):
    """
    Description:
    This function is responsible for drop all tables.

    Args:
        conn: psycopg2 connection
        cur: psycopg2 cursor
    
    Returns:
        None
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(e)


# define create table function
def create_tables(conn, cur):
    """
    Description:
    This function is responsible for create all tables.

    Args:
        conn: psycopg2 connection
        cur: psycopg2 cursor
    
    Returns:
        None
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(e)

# define main function
def main():
    """
    Description:
    This main function is responsible for connect AWS redshift cluster 
    and execute drop_tables and create tables function

    Args:
        None
    
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
        drop_tables(conn,cur)
        print('All tables has been droped')
    except Exception as e:
        print(e)

    
    try:
        create_tables(conn,cur)
        print('All tables has been create')
    except Exception as e:
        print(e)

    conn.close()

if __name__ == '__main__':
    main()