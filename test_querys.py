import unittest
import bd 
import csv


class RasterTest(unittest.TestCase):

    def test_values_file(self):
        # [ [(long,lat), value], ..]
        with open('altifr_p1.txt') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            #line_count = 0
            for line in csv_reader:
                print(line)
                # print (line)
                elt = bd.raster_point_query(float(line[0]),float(line[1]))
                # verifie que elt = resultat attendu a 2 decimales pres
                self.assertAlmostEqual(elt, float(line[2]),2)


if __name__== "__main__" :
    unittest.main()
