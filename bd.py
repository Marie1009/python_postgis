import psycopg2
import rasterio
import unittest
import numpy as np
import timeit
import time
import random
import csv
import matplotlib.pyplot as plt
import psycopg2.extensions
import select

from psycopg2 import OperationalError
from psycopg2 import sql
from psycopg2 import pool
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

def create_connection_pool(min_co, max_co, db_name, db_user, db_password, db_host, db_port):
	try:
		threaded_postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool(min_co, max_co, user = db_user,
		                          password = db_password,
		                          host = db_host,
		                          port = db_port,
		                          database = db_name, async=1)
		if(threaded_postgreSQL_pool):
			print("Connection pool created successfully using ThreadedConnectionPool")

	except (Exception, psycopg2.DatabaseError) as error :
		print ("Error while connecting to PostgreSQL", error)

	return threaded_postgreSQL_pool

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


def get_queries(file):
	with open(file) as csv_file:
		csv_reader = csv.reader(csv_file, delimiter='#')
		qdict = {}
		#liste = []
		#line_count = 0
		for line in csv_reader:
			qdict[line[0]] = line[1]

	return qdict

# exemple de point pour altifr_75m_0150_6825 : 154938.251 6821208.497
def get_image(connection, table, coord_x, coord_y, resolution_x, resolution_y):
	print(table)
	query = "SELECT ST_AsGDALRaster(fct_get_image(ST_GeomFromText('POINT({} {})'), {}, {}, '{}'), 'GTiff')".format(coord_x, coord_y, resolution_x, resolution_y, table)
	
	results = execute_read_query(connection,query)
	for row in results[0]:
	#	print(row)
		rast = bytes(row[0])

		with MemoryFile(rast) as memfile:
			with memfile.open() as dataset:

				data_array = dataset.read(1)
				#read(1) returns numpy array contenant les valeurs de raster pour la bande 1
							
				print(infos_raster(dataset,data_array))

	return results[1]



def start_queries(mode,connection):
	queries_dict = get_queries('queries_not_union.txt')

	keys = list(queries_dict.keys())
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

	elif mode == 3:
		keys.reverse()
		keys_list = keys
		
	f = open("results_sans_union_{}.txt".format(mode), "w")
	for key in keys_list:
		print(key)
		results = execute_read_query(connection,queries_dict[key])
		values = results[0]
		runtime_exe = results[1]
		runtime_fetchall = results[2]
		ratio = (runtime_fetchall / runtime_exe + runtime_fetchall)*100 
		f.write("{} executed in {} seconds and fetched in {} seconds \n ratio : {} % \n".format(key,runtime_exe, runtime_fetchall,ratio))
	f.close()

def plot_perf(times,chart_name):
	plt.ylabel('execution time (seconds)')
	plt.xlabel('N')
	plt.title(chart_name)
	plt.plot( range(len(times)), times, 'b')
	#plt.axis([0, 6, 0, 20])
	plt.savefig("{}.png".format(chart_name))
	#plt.show()
	#print(max(times))
	plt.close()

def plot_fig(t1,t2,t3,t4,chart_name):
	#1 : exe
	#2 : wait
	#3 : fetch
	#4 : total
	plt.figure(1, figsize=(10,10))

	plt.subplots_adjust(hspace=0.7)


	plt.subplot(411)
	plt.ylabel('execution time (s)')
	plt.xlabel('N')
	plt.title('total')
	plt.plot( range(len(t1)), t1, 'b')
	plt.plot( range(len(t2)), t2, 'b')
	plt.plot( range(len(t3)), t3, 'b')
	plt.plot( range(len(t4)), t4, 'r')

	plt.subplot(412)
	plt.ylabel('execution time (s')
	plt.xlabel('N')
	plt.title('execution')
	plt.plot( range(len(t1)), t1, 'b')

	plt.subplot(413)
	plt.ylabel('execution time (s)')
	plt.xlabel('N')
	plt.title('wait')
	plt.plot( range(len(t2)), t2, 'b')

	plt.subplot(414)
	plt.ylabel('execution time (s)')
	plt.xlabel('N')
	plt.title('fetch')
	plt.plot( range(len(t3)), t3, 'b')



	plt.savefig("{}.png".format(chart_name))
	plt.close()

def exe_query_Ntimes_pool(query, N):
	#START CONNECTION POOL
	pool = create_connection_pool(1,5,"postgis_test","postgres","admin","localhost","5432")
	times = []
	#for i in range(N):
	#	t = get_image(connection,'altifr_75m_0150_6825',154938.251,6821208.497, 500, 500)
	#	times.append(t)
	for i in range(N):
		results = query_with_pool(pool,query)
		times.append(results[1])
	pool.closeall
	plot_perf(times,'chart_pool')


def wait(conn):
    while True:
        state = conn.poll()
        if state == psycopg2.extensions.POLL_OK:
            break
        elif state == psycopg2.extensions.POLL_WRITE:
            select.select([], [conn.fileno()], [])
        elif state == psycopg2.extensions.POLL_READ:
            select.select([conn.fileno()], [], [])
        else:
            raise psycopg2.OperationalError("poll() returned %s" % state)

def exe_query_async_Ntimes(query, N):
	aconn = psycopg2.connect(database="postgis_test", user="postgres", host="127.0.0.1", port="5432", password="admin", async=1)
	wait(aconn)
	acurs = aconn.cursor()		

	times_exe = []
	times_fetch = []
	times_wait = []
	times_total = []

	for i in range(N):
		print("Running query {} in async mode".format(i))
		
		start = time.perf_counter()
		acurs.execute(query)
		end = time.perf_counter()
		wait(acurs.connection)
		end_wait = time.perf_counter()
		runtime_exe = end - start

		runtime_wait = end_wait - end

		result = acurs.fetchall()
		end_fetch = time.perf_counter()
		runtime_fetchall = end_fetch - end_wait

		total = end_fetch-start

		times_total.append(total)
		times_exe.append(runtime_exe)
		times_fetch.append(runtime_fetchall)
		times_wait.append(runtime_wait)

	acurs.close()

	plot_fig(times_exe,times_wait,times_fetch,times_total, 'async_perf')
	#plot_perf(times_exe,'async_execution')
	#plot_perf(times_fetch,'async_fetch')
	#plot_perf(times_wait, 'async_wait')
	
def query_async_pool_Ntimes(query,N):
	pool = create_connection_pool(1,5,"postgis_test","postgres","admin","localhost","5432")


	times_exe = []
	times_fetch = []
	times_wait = []
	times_total = []

	for i in range(N):
		print("Running query {} in async mode".format(i))
		aconn  = pool.getconn()
		wait(aconn)
		acurs = aconn.cursor()
		
		start = time.perf_counter()
		acurs.execute(query)
		end = time.perf_counter()
		wait(acurs.connection)
		end_wait = time.perf_counter()
		runtime_exe = end - start

		runtime_wait = end_wait - end

		result = acurs.fetchall()
		end_fetch = time.perf_counter()
		runtime_fetchall = end_fetch - end_wait

		total = end_fetch-start

		times_total.append(total)

		times_exe.append(runtime_exe)
		times_fetch.append(runtime_fetchall)
		times_wait.append(runtime_wait)
	
	#results = execute_read_query(ps_connection, query)
		acurs.close()
		pool.putconn(aconn)

	plot_fig(times_exe,times_wait,times_fetch,times_total, 'async_pool_perf')
	#Use this method to release the connection object and send back ti connection pool

def main():
	#srid = get_raster_srid("altifr_p2")
	#print(srid)

	#START CONNECTION
	#connection = create_connection("postgis_test","postgres","admin","localhost","5432")

	#GET QUERIES FROM FILE AND EXECUTE ALL
	#start_queries(3,connection)
	#connection.close()
	
	
	q1 = "SELECT ST_AsGDALRaster(ST_Union(altifr_75m_0150_6825.rast), 'GTiff') FROM altifr_75m_0150_6825"
	#exe_query_Ntimes_pool(q1, 40)
	exe_query_async_Ntimes(q1,30)
	query_async_pool_Ntimes(q1,30)

if __name__== "__main__" :
	main()
