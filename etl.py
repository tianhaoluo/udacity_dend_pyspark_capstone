import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, IntegerType
from pyspark import SparkContext

config = configparser.ConfigParser()
config.read('dl.cfg')

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
    
    # read from file
    rdd = spark.sparkContext.textFile(filepath)

    # Take care of the multi-character delimiter and rename the columns
    df = rdd.map(lambda line : line.split("::")).toDF().toDF(*cols).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    df.write.mode('overwrite').parquet(os.path.join(output_data,entity_name))
	
def main():
    spark = create_spark_session()
    input_data = config.get('S3','INPUT')
    output_data = config.get('S3','OUTPUT')
    for entity_name in ["movies","users","ratings"]:
         process_data(spark, input_data, output_data, entity_name)    
    spark.stop()

import unittest

class TestStringMethods(unittest.TestCase):

    def test_upper(self):
        self.assertEqual('foo'.upper(), 'FOO')

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
    #main()
    unittest.main()
