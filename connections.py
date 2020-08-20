import psycopg2
import psycopg2.extensions
from psycopg2 import OperationalError
from psycopg2 import sql
from psycopg2 import pool


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

def create_connection_pool(min_co, max_co, db_name, db_user, db_password, db_host, db_port,async):

    if async==1:

        try:
            threaded_postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool(min_co, max_co, user = db_user,
                                      password = db_password,
                                      host = db_host,
                                      port = db_port,
                                      database = db_name, async=1)
            if(threaded_postgreSQL_pool):
                print("ASYNC Connection pool created successfully using ThreadedConnectionPool")

        except (Exception, psycopg2.DatabaseError) as error :
            print ("Error while connecting to PostgreSQL", error)
    else:
        try:
            threaded_postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool(min_co, max_co, user = db_user,
                                      password = db_password,
                                      host = db_host,
                                      port = db_port,
                                      database = db_name)
            if(threaded_postgreSQL_pool):
                print("Connection pool created successfully using ThreadedConnectionPool")

        except (Exception, psycopg2.DatabaseError) as error :
            print ("Error while connecting to PostgreSQL", error)


    return threaded_postgreSQL_pool