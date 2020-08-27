import psycopg2
import rasterio
import unittest
import numpy as np
import time
import csv
import matplotlib.pyplot as plt
import psycopg2.extensions
import select
import psycopg2.extras


import starters 
import connections
import queries

from psycopg2 import OperationalError
from psycopg2 import sql
from psycopg2 import pool
from rasterio.io import MemoryFile




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


def main():
	#srid = get_raster_srid("altifr_p2")
	#print(srid)

	#START CONNECTION
	#connection = create_connection("postgis_test","postgres","admin","localhost","5432")

	#GET QUERIES FROM FILE AND EXECUTE ALL
	#start_queries(3,connection)
	#connection.close()
	

	#module_name = 'psycopg2.extras'
	#module_info = pyclbr.readmodule(module_name)
	#print(module_info)

	#for item in module_info.values():
	#	print(item.name)
	
	q1 = "SELECT ST_AsGDALRaster(ST_Union(altifr_75m_0150_6825.rast), 'GTiff') FROM altifr_75m_0150_6825"
	#starters.exe_query_Ntimes_pool(q1, 5)
	#starters.exe_query_async_Ntimes(q1,5)
	#starters.query_async_pool_Ntimes(q1,30,5)
	#starters.start_multithreading(N,nbthreads,nbpool,query)
	
	#starters.start_multithreading(10,3,5,q1)
	#starters.start_multith_tasks(20,5,5,q1)
	starters.start_multith_tasks_callback(5,100,5,q1)
	#liste =queries.query_overview_table(8,'altifr_p2')
	#print(liste)
if __name__== "__main__" :
	main()
