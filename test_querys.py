import unittest
import bd 

class RasterTest(unittest.TestCase):


    def test_values(self):
    	# [ [(long,lat), value], ..]
        liste = [[(160776.304,6784107.115),74.19999694824219],[(199725.672, 6783417.745), 114.5999984741211],[(175570.932, 6796643.598), 120.19999694824219]]
        elt = bd.raster_point_query(liste[0][0][0],liste[0][0][1])
    	# verifie que elt = resultat attendu a 2 decimales pres
        self.assertAlmostEqual(elt, liste[0][1],2)


        elt = bd.raster_point_query(liste[1][0][0],liste[1][0][1])
    	# verifie que elt = resultat attendu a 2 decimales pres
        self.assertAlmostEqual(elt, liste[1][1],2)

        elt = bd.raster_point_query(liste[2][0][0],liste[2][0][1])
    	# verifie que elt = resultat attendu a 2 decimales pres
        self.assertAlmostEqual(elt, liste[2][1],2)



if __name__== "__main__" :
	unittest.main()
