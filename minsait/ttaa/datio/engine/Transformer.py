import pyspark.sql.functions as f
from pyspark.sql import SparkSession, WindowSpec, Window, DataFrame, Column

from minsait.ttaa.datio.common.Constants import *
from minsait.ttaa.datio.common.naming.PlayerInput import *
from minsait.ttaa.datio.common.naming.PlayerOutput import *
from minsait.ttaa.datio.utils.Writer import Writer


class Transformer(Writer):
    def __init__(self, spark: SparkSession):
        self.spark: SparkSession = spark
        df: DataFrame = self.read_input()
        df = self.clean_data(df)
        self.df = df
    
    def transform(self):
        ''' Transform the data according to requirements from excercises'''
        df = self.df
        df.printSchema()
        # excercise 1
        df = self.column_selection(df)
        # excercise 2
        df = self.window_function(df)
        # excercise 3
        df = self.add_potential_vs_overall(df)
        # excercise 4
        df = self.filter_cat_pot_overall(df)
        # excercise 5
        if FILTER_LOWER_AGE == 1:
            df = self.filter_lower_age(df, FILTER_AGE)

        # for show 100 records after your transformations and show the DataFrame schema
        df.show(n=100, truncate=False)
        df.printSchema()

        # Uncomment when you want write your final output
        self.write(df)

    def read_input(self) -> DataFrame:
        """
        :return: a DataFrame readed from csv file
        """
        return self.spark.read \
            .option(INFER_SCHEMA, True) \
            .option(HEADER, True) \
            .csv(INPUT_PATH)

    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information
        :return: a DataFrame with filter transformation applied
        column team_position != null && column short_name != null && column overall != null
        """
        df = df.filter(
            (short_name.column().isNotNull()) &
            (short_name.column().isNotNull()) &
            (overall.column().isNotNull()) &
            (team_position.column().isNotNull())
        )
        return df

    def column_selection(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information
        :return: a DataFrame with the columns described in excercices
        """
        df = df.select(
            short_name.column(),
            long_name.column(),
            age.column(),
            height_cm.column(),
            weight_kg.column(),
            nationality.column(),
            club_name.column(),
            overall.column(),
            potential.column(),
            team_position.column()
        )
        return df

    def window_function(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information (must have nationality, 
             team_position, and overall columns)
        :return: add to the DataFrame the column "player_cat"
             by each position value
             cat A for if is in top 3 players
             cat B for if is in top 5 players
             cat C for if is in top 10 players
             cat D for the rest
        """
        w: WindowSpec = Window \
            .partitionBy(nationality.column(), team_position.column()) \
            .orderBy(overall.column().desc())
        rank: Column = f.rank().over(w)

        rule: Column = f.when(rank <= 3, "A") \
            .when(rank <= 5, "B") \
            .when(rank <= 10, "C") \
            .otherwise("D")

        df = df.withColumn(player_cat.name, rule)
        return df


    def add_potential_vs_overall(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information (must have potential and overall columns)
        :return: a DataFrame with potential_vs_overall column
        """
        df = df.withColumn(potential_vs_overall.name, 
                           f.round(potential.column() / overall.column(), 3))
        return df


    def filter_cat_pot_overall(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information (must have player_cat 
             and potential_vs_overall columns)
        :return: a DataFrame filtered according to Excercise 4
        """
        df = df.filter((player_cat.column() == "A") | (player_cat.column() == "B") 
                       | ((player_cat.column() == "C") & (potential_vs_overall.column() > 1.15))
                       | ((player_cat.column() == "D") & (potential_vs_overall.column() > 1.25))
        )
        return df


    def filter_lower_age(self, df: DataFrame, limit_age: int) -> DataFrame:
        """
        :param df: is a DataFrame with players information (must have age column)
        :param limit_age: is the limit age (exclusive)
        :return: a DataFrame filtered with age lower than requested
        """
        df = df.filter(age.column() < limit_age)
        return df
