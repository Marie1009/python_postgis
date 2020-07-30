import psycopg2
import rasterio
import cProfile
import pstats
import io
import unittest
import numpy as np
import timeit
import time
import csv

from psycopg2 import OperationalError
from psycopg2 import sql
from rasterio import Affine as A
from rasterio.warp import calculate_default_transform, reproject, Resampling
from rasterio.io import MemoryFile


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

def execute_read_query(connection, query):
	cursor = connection.cursor()
	result = None
	try:
		start = time.perf_counter()
		cursor.execute(query)
		end = time.perf_counter()
		runtime = end - start
		#f.write("{} executed in {} seconds \n".format(key,runtime))

		result = cursor.fetchall()
		return [result,runtime]
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
	

def infos_raster(dataset):
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

def get_queries():
	with open('queries.txt') as csv_file:
		csv_reader = csv.reader(csv_file, delimiter='#')
		qdict = {}
		#liste = []
		#line_count = 0
		for line in csv_reader:
			qdict[line[0]] = line[1]

	return qdict

def start_queries(mode,connection,queries_dict):
	keys = queries_dict.keys()
	print(keys)
	keys_list = []
	#mode "normal" pas de changement on fait les requetes a la suite comme elles sont dans le dict
	if mode == 0:
		keys_list = keys

	#mode double : on fait chaque requete 2 fois a la suite
	elif mode == 1:
		for e in keys:
			keys_list.append(e)
			keys_list.append(e)
			
	#mode aleatoire : on melange les requetes aleatoirement
	elif mode == 2:
		keys_list = random.sample(keys,len(keys))
	

	f = open("results_{}.txt".format(mode), "w")
	for key in keys_list:
		print(key)
		results = execute_read_query(connection,queries_dict[key])
		values = results[0]
		runtime = results[1]
		f.write("{} executed in {} seconds \n".format(key,runtime))
	f.close()

	






def main():
	#srid = get_raster_srid("altifr_p2")
	#print(srid)

	#START CONNECTION
	connection = create_connection("postgis_test","postgres","admin","localhost","5432")
	queries_dict = get_queries()
	
	
	start_queries(0,connection,queries_dict)
	
	#values = test_prepared(connection)

	#end = time.perf_counter()
	#runtime = start - end

#	print(f"Executed in {runtime:0.4f} seconds")

	#for val in values:
#	    print(val)



	#start = time.perf_counter()
	#raster_query()
	#simple_query()
	#print(f"Executed in {time.perf_counter() - start:0.4f} seconds")

    


if __name__== "__main__" :
	main()
