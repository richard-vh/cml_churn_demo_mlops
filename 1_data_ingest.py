# Part 1: Data Ingest
# A data scientist should never be blocked in getting data into their environment,
# so CML is able to ingest data from many sources.
# Whether you have data in .csv files, modern formats like parquet or feather,
# in cloud storage or a SQL database, CML will let you work with it in a data
# scientist-friendly environment.


import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *



spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .master("local[*]")\
    .getOrCreate()

# **Note:**
# Our file isn't big, so running it in Spark local mode is fine but you can add the following config
# if you want to run Spark on the kubernetes cluster
#
# > .config("spark.yarn.access.hadoopFileSystems",os.getenv['STORAGE'])\
#
# and remove `.master("local[*]")\`
#

# Since we know the data already, we can add schema upfront. This is good practice as Spark will
# read *all* the Data if you try infer the schema.

schema = StructType(
    [
        StructField("customerID", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("SeniorCitizen", StringType(), True),
        StructField("Partner", StringType(), True),
        StructField("Dependents", StringType(), True),
        StructField("tenure", DoubleType(), True),
        StructField("PhoneService", StringType(), True),
        StructField("MultipleLines", StringType(), True),
        StructField("InternetService", StringType(), True),
        StructField("OnlineSecurity", StringType(), True),
        StructField("OnlineBackup", StringType(), True),
        StructField("DeviceProtection", StringType(), True),
        StructField("TechSupport", StringType(), True),
        StructField("StreamingTV", StringType(), True),
        StructField("StreamingMovies", StringType(), True),
        StructField("Contract", StringType(), True),
        StructField("PaperlessBilling", StringType(), True),
        StructField("PaymentMethod", StringType(), True),
        StructField("MonthlyCharges", DoubleType(), True),
        StructField("TotalCharges", DoubleType(), True),
        StructField("Churn", StringType(), True)
    ]
)

# Now we can read in the data from Cloud Storage into Spark...

storage = os.environ['STORAGE']

telco_data = spark.read.csv(
    "{}/datalake/data/churn/WA_Fn-UseC_-Telco-Customer-Churn-.csv".format(
        storage),
    header=True,
    schema=schema,
    sep=',',
    nullValue='NA'
)

# ...and inspect the data.

telco_data.show()

telco_data.printSchema()

# Now we can store the Spark DataFrame as a file in the local CML file system
# *and* as a table in Hive used by the other parts of the project.

telco_data.coalesce(1).write.csv(
    "file:/home/cdsw/raw/telco-data/",
    mode='overwrite',
    header=True
)

spark.sql("show databases").show()

spark.sql("show tables in rvh_churn_demo").show()

# Create the Hive table
# This is here to create the table in Hive used be the other parts of the project, if it
# does not already exist.

if ('telco_churn' not in list(spark.sql("show tables in rvh_churn_demo").toPandas()['tableName'])):
    print("creating the telco_churn table")
    telco_data\
        .write.format("parquet")\
        .mode("overwrite")\
        .saveAsTable(
            'rvh_churn_demo.telco_churn'
        )

# Show the data in the hive table
spark.sql("select * from rvh_churn_demo.telco_churn").show()

# To get more detailed information about the hive table you can run this:
spark.sql("describe formatted rvh_churn_demo.telco_churn").toPandas()



