
import psycopg2
import rasterio
from rasterio.io import MemoryFile

conn = psycopg2.connect(database="postgis_test", user="postgres", host="localhost", password="admin")
cur = conn.cursor()
# ensure that the GTiff driver is available, 
# see https://postgis.net/docs/postgis_gdal_enabled_drivers.html

#cur.execute("WITH test as (SELECT ST_Intersection(ST_Union(altifr_p1.rast), ST_Union(altifr_p2.rast)) FROM altifr_p1, altifr_p2)SELECT ST_SummaryStats(	ST_MapAlgebra(	test.st_intersection, 1,test.st_intersection, 2,'([rast2] - [rast1])') ) AS rast FROM test")

cur.execute("SELECT ST_AsGDALRaster(ST_Union(altifr_p1.rast), 'GTiff') FROM altifr_p1")

rows = cur.fetchall()

for row in rows:
	#print(row[0])
	print(row)

	#print(type(row[0]))

	rast = bytes(row[0])

	#print(rast)
	
	with MemoryFile(rast) as memfile:
		with memfile.open() as dataset:
			data_array = dataset.read()
			#read() returns numpy array
			print("dataset name : ")
			print(dataset.name)

			print(data_array)
			
			print(dataset.bounds)

			print("dataset bands : ")
			print(dataset.indexes)
			
	