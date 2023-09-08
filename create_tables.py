"""
This script is used to connect to the database, 
drop tables if they already exist and create the fact 
and dimension tables for the star schema in Redshift

author: Philippe Jean Mith
date: Sept 9th 2023
"""

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    fonction to drop the tables

    Input
    -----
        cur: cursor object
            Allows Python code to execute query in a database session
        conn: connection object
             Handles the connection to the database instance.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    fonction to create the tables

    Input
    -----
        cur: cursor object
            Allows Python code to execute query in a database session
        conn: connection object
             Handles the connection to the database instance.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():

    """
    This function is used to connection with sparkify database and gets cursor to it, 
    drops all the tables if already existed, creates all tables needed and finally closes connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        f"host={config['CLUSTER']['HOST']} dbname={config['CLUSTER']['DB_NAME']} \
              user={config['CLUSTER']['DB_USER']} \
             password={config['CLUSTER']['DB_PASSWORD']} port={config['CLUSTER']['DB_PORT']}")
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()