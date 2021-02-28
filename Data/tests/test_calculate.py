import unittest
import sys
sys.path.insert(0,'..')
import calculate


class TestCalc(unittest.TestCase):

	def test_load_csvs(self):
		result = calculate.load_csvs("../csv/", "../duplicates/")
		self.assertIsNotNone(result, 'Required files not loaded')


if __name__ == '__main__':
	unittest.main()