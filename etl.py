"""
This script is used to load data from S3 into staging tables on Redshift 
and then process that data into your analytics tables on Redshift.

author: Philippe Jean Mith
date: Sept 9th 2023
"""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    fonction to load the stage tables

    Input
    -----
        cur: cursor object
            Allows Python code to execute query in a database session
        conn: connection object
             Handles the connection to the database instance.
    """
    for query in copy_table_queries:
        print('Stage tables are loading...')
        cur.execute(query)
        conn.commit()
        print('Stage tables loaded successfully!')


def insert_tables(cur, conn):
    """
    fonction to load the fact and dimension tables

    Input
    -----
        cur: cursor object
            Allows Python code to execute query in a database session
        conn: connection object
             Handles the connection to the database instance.
    """
    for query in insert_table_queries:
        print('Fact and dimenssion tables are loading...')
        cur.execute(query)
        conn.commit()
        print('Fact and dimenssion tables loaded successfully!')


def main():
    """
    This function is used to connection with sparkify database and gets cursor to it, 
    load the stage tables and load the fact and dimension tables.
    """
    # Get the connection information
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # Connect to the database
    conn = psycopg2.connect(f"host={config['CLUSTER']['HOST']} \
                            dbname={config['CLUSTER']['DB_NAME']} user={config['CLUSTER']['DB_USER']} \
             password={config['CLUSTER']['DB_PASSWORD']} port={config['CLUSTER']['DB_PORT']}")
    cur = conn.cursor()
    # Load the stage tables
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    # Close the connection
    conn.close()


if __name__ == "__main__":
    main()