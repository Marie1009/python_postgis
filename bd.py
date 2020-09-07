import psycopg2
import rasterio
import unittest
import numpy as np
import time
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from operator import add

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

def plot_points(points_conn, point_exe, point_wait,chart_name):
    #plt.ylabel('execution time (seconds)')
    plt.xlabel('s')
    plt.title(chart_name)
    #plt.plot( points_conn,range(len(points_conn)), 'b+')
    #plt.plot( point_exe,range(len(point_exe)), 'g1')
    #plt.plot( point_wait,range(len(point_wait)), 'rx')
    plt.plot( points_conn, 'b+')
    plt.plot( point_exe, 'g1')
    plt.plot( point_wait,'rx')
    #plt.axis([0, 6, 0, 20])
    plt.savefig("{}.png".format(chart_name))
    #plt.show()
    #print(max(times))
    plt.close()

def plot_start_end_phases(starts, ends,  wends,  fends,chart_name):
	#plt.ylabel('execution time (seconds)')
	plt.figure(1, figsize=(4,2.5))
	plt.xlabel('time (s)')
	plt.ylabel('query')

	plt.title(chart_name)

	total = max(fends) - min(starts) 	

	sizes = []
	for i in range(len(starts)):
		sizes.append(ends[i]-starts[i]) 

	for i in range(len(starts)-1):
		starts[i+1] = starts[i+1]- starts[0]
	starts[0]=0

	
	wait_sizes = []
	for i in range(len(ends)):
		wait_sizes.append(wends[i]-ends[i]) 
	
	
	fetch_sizes = []
	for i in range(len(wends)):
		fetch_sizes.append(fends[i]-wends[i]) 

	#for i in range(len(fstarts)-1):
	#	fstarts[i+1] = fstarts[i+1]- fstarts[0]
	#fstarts[0]=0
	plt.barh(range(len(sizes)),sizes,height=0.5,left=starts,color='b')
	
	left_wait = list( map(add, sizes, starts) )
	

	plt.barh(range(len(wait_sizes)),wait_sizes,height=0.5,left=left_wait,color='r')

	
	left_fetch = list( map(add, left_wait, wait_sizes) )
	

	plt.barh(range(len(fetch_sizes)),fetch_sizes,height=0.5,left=left_fetch,color='g')

	red_patch = mpatches.Patch(color='red', label='wait')
	green_patch = mpatches.Patch(color='green', label='fetch')
	blue_patch = mpatches.Patch(color='blue', label='get connection\nstart query')

	plt.legend(handles=[blue_patch,red_patch,green_patch])

	print("total {} s".format(total))

	plt.savefig("{}.png".format(chart_name),bbox_inches = "tight")
	
	plt.close()

def main():
	
	q1 = "SELECT ST_AsGDALRaster(ST_Union(o_16_altifr_75m_0150_6825.rast), 'GTiff') FROM o_16_altifr_75m_0150_6825"
	q2 = "SELECT ST_AsGDALRaster(o_16_demtable.rast, 'GTiff') FROM o_16_demtable"

	#starters.exe_query_Ntimes_pool(q1, 5)
	#starters.exe_query_async_Ntimes(q1,5)
	#starters.query_async_pool_Ntimes(q1,30,5)
	#starters.start_sync_file_queries(0,'queries.txt','sync_execution')
	
	#conn = connections.create_connection("postgis_test","postgres","admin","localhost","5432")
	#queries.execute_read_query(conn,q1)
	#starters.start_multith_file(10,10,'queries.txt','async_execution')
	starters.query_table_overviews(16,'altifr_75m_0150_6825',0,10)
	starters.query_table_overviews(16,'altifr_75m_0150_6825',5,10)
	

if __name__== "__main__" :
	main()
