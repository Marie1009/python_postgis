import unittest
import bd 

class RasterTest(unittest.TestCase):

	def test_values_file(self):
		# [ [(long,lat), value], ..]
		f = open("altifr_p1.txt","r")
		lines =f.readlines()
		for line in lines:
			print(line)
			liste=list(line.split(", "))
			elt = bd.raster_point_query(float(liste[0]),float(liste[1]))
		# verifie que elt = resultat attendu a 2 decimales pres
			self.assertAlmostEqual(elt, float(liste[2]),2)


if __name__== "__main__" :
	unittest.main()
