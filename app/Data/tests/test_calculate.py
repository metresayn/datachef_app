import unittest
import sys
sys.path.insert(0,'..')
import calculate


class TestCalc(unittest.TestCase):

	def test_load_csvs(self):
		result = calculate.load_csvs("../csv/", "../duplicates/")
		self.assertIsNotNone(result, 'Required files not loaded')
		
	def test_create_banner_campaign_dataframe(self):
		subs_df = calculate.load_csvs("../test_csv/", "../test_duplicates/")
		result_df = calculate.create_banner_campaign_dataframe(subs_df, '1')
		#print (result_df)
		self.assertIsNotNone(result_df, 'Intermediate dataframe was not loaded')

if __name__ == '__main__':
	unittest.main()