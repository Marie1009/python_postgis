import psycopg2
import rasterio
import cProfile
import pstats
import io
import unittest
import numpy as np

from rasterio import Affine as A
from rasterio.warp import calculate_default_transform, reproject, Resampling
from rasterio.io import MemoryFile


# QUERYS SANS RASTER OUTPUT
def simple_query():
	# ensure that the GTiff driver is available, 
	# see https://postgis.net/docs/postgis_gdal_enabled_drivers.html
	conn = psycopg2.connect(database="postgis_test", user="postgres", host="localhost", password="admin")
	cur = conn.cursor()

	# query pour recuperer des stats simples sur une intersection de 2 raster (calcul rast2 - rast1) :
	#cur.execute("WITH test as (SELECT ST_Intersection(ST_Union(altifr_p1.rast), ST_Union(altifr_p2.rast)) FROM altifr_p1, altifr_p2)SELECT ST_SummaryStats(	ST_MapAlgebra(	test.st_intersection, 1,test.st_intersection, 2,'([rast2] - [rast1])') ) AS rast FROM test")

	# query qui recupere les valeurs des pixel pour une paire lat/long donnee : 
	cur.execute("SELECT ST_Value(altifr_p1.rast, ST_GeomFromText('POINT(175570.932 6796643.598)',2154)) FROM altifr_p1")

	rows = cur.fetchall()

	for row in rows:
		print(row)

def get_raster_srid(table):
	conn = psycopg2.connect(database="postgis_test", user="postgres", host="localhost", password="admin")
	cur = conn.cursor()
	# query qui recupere les valeurs des pixel pour une paire lat/long donnee : 
	cur.execute("SELECT ST_SRID(ST_Union("+ table +".rast)) FROM "+table)
	rows = cur.fetchall()
	return rows[0][0]
	

# QUERY AVEC RASTER OUTPUT :
def raster_query():
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
		#print(type(row[0]))
		rast = bytes(row[0])

		with MemoryFile(rast) as memfile:
			with memfile.open() as dataset:

				data_array = dataset.read(1)
				#read(1) returns numpy array contenant les valeurs de raster pour la bande 1

				print(dataset.crs)
				if dataset.crs != "ESPG:4326":
					dst_crs = 'EPSG:4326'

					#transform, width, height = calculate_default_transform(dataset.crs, dst_crs, dataset.width, dataset.height, *dataset.bounds)
				    
					#kwargs = dataset.meta.copy()
					#kwargs.update({  'crs': dst_crs, 'transform': transform, 'width': width,  'height': height  })
					#dst = np.empty([750,1000])
					#reproject(   source=data_array, destination=dst,  src_transform=dataset.transform, src_crs=dataset.crs, dst_transform=transform, dst_crs=dst_crs, resampling=Resampling.nearest)

				

				print(data_array.shape)
				#print(data_array)

				#print(dst.shape)
				print(data_array)
				#print(data_array[0])
				print(dataset.bounds)
				
				print(dataset.dtypes)
				#print(dataset.xy(dataset.height // 2, dataset.width // 2))

				longi = 160776.304
				lati = 6784107.115
				print(longi)
				print(lati)

				#longi = 175570.932
				#lati = 6796643.598

				indices = dataset.index(longi,lati)
				x = indices[0]
				y = indices[1]
				
				#print(dataset.xy(x,y))
				#print(dataset.xy(x+1,y+1))
				#print(dataset.xy(x-1,y-1))

				value = data_array[int(x+1)][int(y+1)]
				print( value )


# QUERY AVEC RASTER OUTPUT :
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
		#print(type(row[0]))
		rast = bytes(row[0])

		with MemoryFile(rast) as memfile:
			with memfile.open() as dataset:
				data_array = dataset.read(1)
				#read(1) returns numpy array contenant les valeurs de raster pour la bande 1
				#print(dataset.crs)
							
				indices = dataset.index(longi,lati)
				x = indices[0]
				y = indices[1]
				
				value = data_array[int(x)][int(y)]
				print( value )

	return value 


def main():
	#srid = get_raster_srid("altifr_p2")
	#print(srid)
	#cProfile.run('raster_query()')
    raster_query()
    


if __name__== "__main__" :
	main()
