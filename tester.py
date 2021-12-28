from pyspark.sql import SparkSession

from minsait.ttaa.datio.common.Constants import *
from minsait.ttaa.datio.engine.Transformer import Transformer
from minsait.ttaa.datio.common.naming.PlayerInput import *
from minsait.ttaa.datio.common.naming.PlayerOutput import *
import unittest;

spark: SparkSession = SparkSession \
    .builder \
    .master(SPARK_MODE) \
    .getOrCreate()
transformer = Transformer(spark)

class TestTransformer(unittest.TestCase):

    def test_exe1(self):
        # COMPLETE TEST
        transformer.df = transformer.column_selection(transformer.df)
        cols = transformer.df.columns
        # Verify if all requested columns are in the selection 
        self.assertTrue('short_name' in cols)
        self.assertTrue('long_name' in cols)
        self.assertTrue('age' in cols)
        self.assertTrue('height_cm' in cols)
        self.assertTrue('weight_kg' in cols)
        self.assertTrue('nationality' in cols)
        self.assertTrue('club_name' in cols)
        self.assertTrue('overall' in cols)
        self.assertTrue('potential' in cols)
        self.assertTrue('team_position' in cols)
        # Also, verify that those are ALL the columns in selection
        self.assertEqual(len(cols), 10)

    def test_exe2(self):
        # Incomplete test for excercise 2, but a basic test to verify that
        # the new column was generated
        transformer.df = transformer.window_function(transformer.df)
        self.assertTrue('player_cat' in transformer.df.columns)

    def test_exe3(self):
        # Incomplete test for excercise 2, but a basic test to verify that
        # the new column was generated
        transformer.df = transformer.add_potential_vs_overall(transformer.df)
        self.assertTrue('potential_vs_overall' in transformer.df.columns)

    def test_exe4(self):
        # COMPLETE TEST
        # Filter according to exe 4
        transformer.df = transformer.filter_cat_pot_overall(transformer.df)
        # If we make the inverse filter, we should have 0 tuples in dataframe
        df = transformer.df
        df = df.filter(((player_cat.column() == "C") & (potential_vs_overall.column() <= 1.15))
                       | ((player_cat.column() == "D") & (potential_vs_overall.column() <= 1.25))
        )
        self.assertEqual(df.count(), 0)

    def test_exe5(self):
        # COMPLETE TEST
        if FILTER_LOWER_AGE == 1:
            transformer.df = transformer.filter_lower_age(transformer.df, FILTER_AGE)
            df = transformer.df
            # Inverse filter, tuples with age >= than requested
            df = df.filter(age.column() >= FILTER_AGE)
            self.assertEqual(df.count(), 0)

if __name__ == '__main__':
    unittest.main()
    