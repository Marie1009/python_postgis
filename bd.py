
import psycopg2
import rasterio
from rasterio.io import MemoryFile

from matplotlib import pyplot

conn = psycopg2.connect(database="postgis_test", user="postgres", host="localhost", password="admin")
cur = conn.cursor()

# ensure that the GTiff driver is available, 
# see https://postgis.net/docs/postgis_gdal_enabled_drivers.html

# QUERYS SANS RASTER OUTPUT

# query pour recuperer des stats simples sur une intersection de 2 raster (calcul rast2 - rast1) :
#cur.execute("WITH test as (SELECT ST_Intersection(ST_Union(altifr_p1.rast), ST_Union(altifr_p2.rast)) FROM altifr_p1, altifr_p2)SELECT ST_SummaryStats(	ST_MapAlgebra(	test.st_intersection, 1,test.st_intersection, 2,'([rast2] - [rast1])') ) AS rast FROM test")

# query qui recupere les valeurs des pixel pour une paire lat/long donnee : 
#cur.execute("SELECT ST_Value(altifr_p1.rast, ST_GeomFromText('POINT(175570.932 6796643.598)',2154)) FROM altifr_p1")

# UTILISER LES 3 LIGNES CI DESSOUS

#rows = cur.fetchall()

#for row in rows:
#	print(row)


# QUERY AVEC RASTER OUTPUT :

#query qui retourne une table raster

cur.execute("SELECT ST_AsGDALRaster(ST_Union(altifr_p1.rast), 'GTiff') FROM altifr_p1")


rows = cur.fetchall()

for row in rows:
	print(row)

	#print(type(row[0]))
	rast = bytes(row[0])

	with MemoryFile(rast) as memfile:
		with memfile.open() as dataset:

			data_array = dataset.read(1)
			#read(1) returns numpy array contenant les valeurs de raster pour la bande 1

			print(data_array.shape)
			print(data_array)

			#print(data_array[0])
			
			
			print(dataset.bounds)

			print(dataset.dtypes)
			#print(dataset.xy(dataset.height // 2, dataset.width // 2))

			longi = 175570.932
			lati = 6796643.598

			indices = dataset.index(longi,lati)
			x = indices[0]
			#print(x)
			y = indices[1]
			#print(y)
			print(dataset.xy(x,y))
			print(data_array[int(x)][int(y)])
			#pyplot.imshow(data_array, cmap='pink')
			#<matplotlib.image.AxesImage object at 0x...>
			#pyplot.show() 

			#for val in data_array.sample([(300, 300)]): 
			#	print(val)
			
			