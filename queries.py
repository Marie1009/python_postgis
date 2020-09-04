
import psycopg2
import psycopg2.extensions
from psycopg2 import OperationalError
from psycopg2 import sql
from psycopg2 import pool
import time
import bd
import connections
import starters
import math
import sys
from rasterio.io import MemoryFile

def query_with_pool(pool, query):

    # Use getconn() method to Get Connection from connection pool
    ps_connection  = pool.getconn()

    if(ps_connection):

        print("successfully recived connection from connection pool ")
        results = execute_read_query(ps_connection, query)
        
    #Use this method to release the connection object and send back ti connection pool
    pool.putconn(ps_connection)
    print("Put away a PostgreSQL connection")

    return results


def execute_read_query(connection, query):
    #cursor = connection.cursor()
    result = None
    try:
        with connection:
            with connection.cursor() as curs:
                start = time.perf_counter()
                curs.execute(query)
                end = time.perf_counter()
                runtime_exe = end - start
                #f.write("{} executed in {} seconds \n".format(key,runtime))

                result = curs.fetchall()
                end_fetch = time.perf_counter()
                runtime_fetchall = end_fetch - end

                curs.close()

        return [result,runtime_exe, runtime_fetchall]
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def execute_read_query_param(connection,champ, table):
    query = sql.SQL("select ST_AsGDALRaster(ST_Union({field}), 'GTiff') from {table}").format(field=sql.Identifier(champ),table=sql.Identifier(table))
    #pkey=sql.Identifier('id'))
    #query = sql.SQL("select {field} from {table} where {pkey} = %s").format(
    #field=sql.Identifier('my_name'),
    #table=sql.Identifier('some_table'),
    #pkey=sql.Identifier('id'))

    #query = sql.SQL("select {fields} from {table}").format(
    #fields=sql.SQL(',').join([
        #sql.Identifier('field1'),
        #sql.Identifier('field2'),
        #sql.Identifier('field3'),
    #]),
    #table=sql.Identifier('some_table'))
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def get_raster_srid(table):
    conn = psycopg2.connect(database="postgis_test", user="postgres", host="localhost", password="admin")
    cur = conn.cursor()
    # query qui recupere les valeurs des pixel pour une paire lat/long donnee : 
    cur.execute("SELECT ST_SRID(ST_Union("+ table +".rast)) FROM "+table)
    rows = cur.fetchall()
    return rows[0][0]
    

def infos_raster(dataset,data_array):
    return [dataset.crs, data_array.shape,dataset.bounds, dataset.transform, dataset.dtypes]
    

# QUERY POUR TESTER VALEURS DANS TABLE altifr_p1 :
def raster_point_query(longi,lati):
    # ensure that the GTiff driver is available, 
    # see https://postgis.net/docs/postgis_gdal_enabled_drivers.html
    conn = psycopg2.connect(database="postgis_test", user="postgres", host="localhost", password="admin")
    cur = conn.cursor()

    #query qui retourne une table raster
    #avec ST_Union : retourne un raster sans tuiles 750x1000
    cur.execute("SELECT ST_AsGDALRaster(ST_Union(altifr_p1.rast), 'GTiff') FROM altifr_p1")

    #sans union on recupere le raster decoupe en tuiles dans ce cas 250x250
    #cur.execute("SELECT ST_AsGDALRaster(altifr_p1.rast, 'GTiff') FROM altifr_p1")
    rows = cur.fetchall()
    value = 0 

    for row in rows:
        print(row)
        rast = bytes(row[0])

        with MemoryFile(rast) as memfile:
            with memfile.open() as dataset:

                data_array = dataset.read(1)
                #read(1) returns numpy array contenant les valeurs de raster pour la bande 1
                            
                indices = dataset.index(longi,lati)
                #index(longi,lati) donne la position x,y du point dans le numpy array

                x = indices[0]
                y = indices[1]
                
                value = data_array[int(x)][int(y)]
                print( value )
    return value 


# exemple de point pour altifr_75m_0150_6825 : 154938.251 6821208.497
def get_image(connection, table, coord_x, coord_y, resolution_x, resolution_y):
    print(table)
    query = "SELECT ST_AsGDALRaster(fct_get_image(ST_GeomFromText('POINT({} {})'), {}, {}, '{}'), 'GTiff')".format(coord_x, coord_y, resolution_x, resolution_y, table)
    
    results = execute_read_query(connection,query)
    for row in results[0]:
    #   print(row)
        rast = bytes(row[0])

        with MemoryFile(rast) as memfile:
            with memfile.open() as dataset:

                data_array = dataset.read(1)
                #read(1) returns numpy array contenant les valeurs de raster pour la bande 1
                            
                print(infos_raster(dataset,data_array))

    return results[1]

def table_overviews_list(max_o,table):
    table_names = []
    table_names.append(table)

    powers = [i for i in range(2, max_o+1) if (math.log(i)/math.log(2)).is_integer()]
    for p in powers:
        table_names.append("o_{}_{}".format(p,table))
        
    table_names.reverse()
#on renvoie la liste reverse pour qu'elle commence par les overviews les - detaillees
    return table_names

def table_overviews_dict(max_o,table):
    table_names = {}
    table_names[0] = table

    powers = [i for i in range(2, max_o+1) if (math.log(i)/math.log(2)).is_integer()]
    for p in powers:
        table_names[p]= "o_{}_{}".format(p,table)
        
    return table_names

def test_raster_results(results):
    for row in results:
    #   print(row)
        rast = bytes(row[0])

        with MemoryFile(rast) as memfile:
            with memfile.open() as dataset:

                data_array = dataset.read(1)
                #read(1) returns numpy array contenant les valeurs de raster pour la bande 1
                            
                print(infos_raster(dataset,data_array))