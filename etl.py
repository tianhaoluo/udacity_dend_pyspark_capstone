import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, IntegerType, StringType
from pyspark import SparkContext
import unittest
import pandas as pd

config = configparser.ConfigParser()
config.read('dl.cfg')
age_dict = { 1:  "Under 18",
	    18:  "18-24",
	    25:  "25-34",
	    35:  "35-44",
	    45:  "45-49",
	    50:  "50-55",
	    56:  "56+"}


occupation_dict = {0:  "other",
	         1:  "academic/educator",
                 2:  "artist",
                 3:  "clerical/admin",
                 4:  "college/grad student",
                 5:  "customer service",
                 6:  "doctor/health care",
                 7:  "executive/managerial",
                 8:  "farmer",
                 9:  "homemaker",
                10:  "K-12 student",
                11:  "lawyer",
                12:  "programmer",
                13:  "retired",
                14:  "sales/marketing",
                15:  "scientist",
                16:  "self-employed",
                17:  "technician/engineer",
                18:  "tradesman/craftsman",
                19:  "unemployed",
                20:  "writer"}

def create_spark_session():
    """Create a spark session
    
        Args:
            - None
        
        Return:
            - An instance of SparkSession
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark
	
def process_data(spark, input_data, output_data, entity_name):
    """Read the data from DAT file, produce a relevant table and write them back into S3
    
        Args:
            - spark: current instance of a SparkSession, created by 'create_spark_session' function
            - input_data: A string, the path to the input data on S3
            - output_data: A string, the path to the output data on S3
			- entity_name: A string, can be 'movies', 'users' or 'ratings'
        Return:
            - Nothing
    """
    # get filepath to song data file
	
    if entity_name == "movies":
        cols = ["movie_id","title","genres"]
    elif entity_name == "users":
        cols = ["user_id","gender", "age", "occupation","zipcode"]
    else:
        cols = ["user_id","movie_id","rating","timestamp"]
	
	
    filepath = os.path.join(input_data, entity_name) +  ".dat"
    
    # read from file, get an intermediate RDD with '::' replaced by '\t' since Spark currently does not support delimiter with multiple characters
    rdd = spark.sparkContext.textFile(filepath)\
            .map(lambda line : '\t'.join(line.split("::")))

    # Read as if it is a tab delimited file and change column names
    df = spark.read.option("inferSchema", "true").option("delimiter", "\t").csv(rdd).toDF(*cols).dropDuplicates()
    
    #Register a UDF to change the occupation column from integer code to string
    occupation_code_str = udf(lambda code: occupation_dict.get(code,"unknown"), StringType()) 
    #special cases:
    if entity_name == "users":
        df = df.withColumn('occupation_name',occupation_code_str('occupation')).drop('occupation').withColumnRenamed('occupation_name','occupation')
    # write songs table to parquet files partitioned by year and artist
    df.write.mode('overwrite').parquet(os.path.join(output_data,entity_name))
	
def main():
    """Processing the main data pipeline, generate a dictionary of pandas dataframe for testing
    
        Args:
            None
        Return:
            test_dfs, type dict[pd.dataFrame]
    """
    spark = create_spark_session()
    test_dfs = {}
    input_data = config.get('S3','INPUT')
    output_data = config.get('S3','OUTPUT')
    for entity_name in ["movies","users","ratings"]:
         process_data(spark, input_data, output_data, entity_name)
         test_dfs[entity_name] = spark.read.parquet(os.path.join(output_data,entity_name)).limit(20).toPandas()  
    spark.stop()
    return test_dfs


class TestMethods(unittest.TestCase):
    def test_hasrow(self):
         self.assertTrue(movies.shape[0] > 0)

    def test_isupper(self):
        self.assertTrue('FOO'.isupper())
        self.assertFalse('Foo'.isupper())

    def test_split(self):
        s = 'hello world'
        self.assertEqual(s.split(), ['hello', 'world'])
        # check that s.split fails when the separator is not a string
        with self.assertRaises(TypeError):
            s.split(2)

if __name__ == "__main__":
    test_dfs = main()
    movies = test_dfs["movies"]
    ratings = test_dfs["ratings"]
    users = test_dfs["users"]
    unittest.main()
