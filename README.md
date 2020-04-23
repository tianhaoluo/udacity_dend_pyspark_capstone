# Introduction

GroupLens Research has collected and made available rating data sets from the MovieLens web site (http://movielens.org). The data sets were collected over various periods of time, depending on the size of the set. For this DEMO, I am using the dataset version with 1 million ratings. However, this project can be easily extended to the versions with larger sizes.

# How to run the script

1. SSH into your EMR cluster, create an S3 bucket for later use of dumping output files (for example, an S3 bucket with name 'udacity-yourname-cfnsadcnd'). You might need to update pip install the package 'configparser' by running `pip install --user configparser`.

2. Go to the configuration file 'dl.cfg' to update your AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, INPUT, OUTPUT. It is recommended that you do not put any quotes around the strings. For example INPUT=s3a://udacity-dend/ NOT INPUT="s3a://udacity-dend/". OUTPUT can be the S3 bucket you just created, e.g. OUTPUT=s3a://udacity-yourname-cfnsadcnd/.

3. Locate where spark-submit is with the command which spark-submit On an EMR cluster, it usually should be /usr/bin/spark-submit

4. Open the terminal, run /usr/bin/spark-submit etl.py

5. Enjoy your new Data Lake for MovieLens! For example, you can load the songplays_table by running in Jupyter Notebook with a PySpark kernel

```
config = configparser.ConfigParser()
config.read('dl.cfg')
output_data = config.get('S3','OUTPUT')
ratings  = spark.read.parquet(os.path.join(output_data,"ratings"))
```

Then you can load other tables as well for some queries!


# Step 1: Scope the Project and Gather Data
## Scope
Q: Explain what you plan to do in the project in more detail. What data do you use? What is your end solution look like? What tools did you use? 

A: I plan to read the MovieLens data from the S3 bucket. For this particular example, I am using the "MovieLens 1M Dataset" https://grouplens.org/datasets/movielens/ but we can potentially increase the size of the data with the same pipeline. Spark is the tool I use since it takes advantage of in-memory computation and lazy evaluation to hand large datasets.

## Describe and Gather Data
Q: Describe the data sets you're using. Where did it come from? What type of information is included?

A: The datasets come from https://grouplens.org/datasets/movielens/. Apart from the original data included, they also have a README file which maps the integer occupation code in the original dataset to actual string descriptions, e.g. 1 = "academic/educator".

## Step 2: Explore and Assess the Data
# Explore the Data
Q: Identify data quality issues, like missing values, duplicate data, etc.

A: The data quality is high overall for this data. Since in movie titles, it's likely to contain "," and ":" and for movie genres, it's likely to have "|" when multiple genres are existing in the same movie. These characters therefore cannot be used as delimiter, the original data opted to use "::" as delimiter, which makes it not possible to be directly handled by pyspark. Workaround needed. For null values, not all users chose to provide the complete demographic information.

# Cleaning Steps
Document steps necessary to clean the data

1. Since "::" isn't supported natively as a delimiter in pyspark, I opted to read in the file as a textFile (output is an RDD with multiple lines of strings), then I replace "::" to "\t" (fortunately "\t" is not in the original .dat files). Then the RDD can be read in as a csv file albeit it is tab delimitered.

2. Replace the integer occupation code with string with the following mapping

	*  0:  "other" or not specified
	*  1:  "academic/educator"
	*  2:  "artist"
	*  3:  "clerical/admin"
	*  4:  "college/grad student"
	*  5:  "customer service"
	*  6:  "doctor/health care"
	*  7:  "executive/managerial"
	*  8:  "farmer"
	*  9:  "homemaker"
	* 10:  "K-12 student"
	* 11:  "lawyer"
	* 12:  "programmer"
	* 13:  "retired"
	* 14:  "sales/marketing"
	* 15:  "scientist"
	* 16:  "self-employed"
	* 17:  "technician/engineer"
	* 18:  "tradesman/craftsman"
	* 19:  "unemployed"
	* 20:  "writer"

3. Most zipcode is in "xxxxx" format but there are a bunch of them in "xxxxx-xxxx" format. For consistency, convert all zipcode to the former format.

4. The original timestamp is an integer with UTC timestamp, I converted it into a TimestampType()

5. Add an monotonicallyIncreasingId() to be the ratings_id column for the ratings column.

# Step 3: Define the Data Model¶
## 3.1 Conceptual Data Model
Q: Map out the conceptual data model and explain why you chose that model

A: I opted to use the STAR schema, where the fact table is the ratings table which has the schema

```
root
 |-- rating_id: long (nullable = true)
 |-- user_id: integer (nullable = true)
 |-- movie_id: integer (nullable = true)
 |-- rating: integer (nullable = true)
 |-- datetime: timestamp (nullable = true)
```
the dimension table users has the schema

```
root
 |-- user_id: integer (nullable = true)
 |-- gender: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- occupation: string (nullable = true)
 |-- zipcode: string (nullable = true)
```
and for the dimension table movies

```
root
 |-- movie_id: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- genres: string (nullable = true)
```

# 3.2 Mapping Out Data Pipelines
Q: List the steps necessary to pipeline the data into the chosen data model

A: Pretty straightforward, but I do need to

1. Read in the data with a sparkSession instance.
2. Do the transformations described above in Step 2.
3. Write the pyspark dataframes back to S3 in parquet.

# Step 4: Run Pipelines to Model the Data
## 4.1 Create the data model
Done! Spark's Schema On Read is really powerful such that we don't need to spend too much time specifying the type of each column.

## 4.2 Data Quality Checks
Q: Explain the data quality checks you'll perform to ensure the pipeline ran as expected. These could include:

A: Integrity constraints on the relational database (e.g., unique key, data type, etc.)

Q:Unit tests for the scripts to ensure they are doing the right thing
Source/Count checks to ensure completeness

A: For Spark, there isn't too many integrity constraints to check, since it is inferring schema automatically and I believe for this task, it is doing a great job!
Unit tests written to check the uniqueness of key and the completeness of data. DONE!

## 4.3 Data dictionary
Q: Create a data dictionary for your data model. For each field, provide a brief description of what the data is and where it came from. You can include the data dictionary in the notebook or in a separate file.

A: Refer to the schemas shown in Section 3.1. 

For ratings table, 'ratings_id' is like the primary key and 'user_id' and 'movie_id' are used to link dimension tables 'users' and 'movies' respectively.

For users table, 'user_id' plays a similar role of primary key in the relational database. For movies table, 'movie_id' is similar to primary key.

# Step 5: Complete Project Write Up
## Clearly state the rationale for the choice of tools and technologies for the project.
Q: Propose how often the data should be updated and why.

A: As often as the data provider update their dataset, which is not very often. If we opt to run the latest version of the dataset, we can run it every several months, whenever the providers are updating the data.


## Write a description of how you would approach the problem differently under the following scenarios:

Q: The data was increased by 100x.

A: Use a more advanced version of AWS EMR cluster.

Q: The data populates a dashboard that must be updated on a daily basis by 7am every day.

A: Incorporate Airflow to schedule the task. I didn't use Airflow for this project since pyspark alone is sufficient for this specific task.

Q: The database needed to be accessed by 100+ people.

A: Just give appropriate permission to my team for the S3 bucket. (Give most of them read access, give the core development team read and write access)


