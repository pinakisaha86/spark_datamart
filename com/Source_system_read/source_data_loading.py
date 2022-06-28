from pyspark.sql import *
from pyspark.sql functions import *
import yaml
import os.path
from com.utility.utils import *

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "mysql:mysql-connector-java:8.0.15" pyspark-shell'
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    s3_bucket= app_conf['s3_conf']['s3_bucket']
    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

# read data from mysql, create data frame out of it and write it into aws s3 bucket
    jdbc_params = {"url": ut.get_mysql_jdbc_url(app_secret),
                  "lowerBound": "1",
                  "upperBound": "100",
                  "dbtable": app_conf["mysql_conf"]["dbtable"],
                  "numPartitions": "2",
                  "partitionColumn": app_conf["mysql_conf"]["partition_column"],
                  "user": app_secret["mysql_conf"]["username"],
                  "password": app_secret["mysql_conf"]["password"]
                   }


    print("\nReading data from MySQL DB using SparkSession.read.format(),")
    txn_df = read_from_mysql(spark, jdbc_params)
        .withcolumn('ins_dt', current_date())

    txn_df.show()
    txn_df.write.parquet('s3://' +s3_bucket + '/folder')

# spark-submit --packages "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1" dataframe/ingestion/others/systems/mongo_df.py
#read data from sftp, create data frame out of it and write it into aws s3 bucket
 ol_txn_df = spark.read\
        .format("com.springml.spark.sftp")\
        .option("host", app_secret["sftp_conf"]["hostname"])\
        .option("port", app_secret["sftp_conf"]["port"])\
        .option("username", app_secret["sftp_conf"]["username"])\
        .option("pem", os.path.abspath(current_dir + "/../../../../" + app_secret["sftp_conf"]["pem"]))\
        .option("fileType", "csv")\
        .option("delimiter", "|")\
        .load(app_conf["sftp_conf"]["directory"] + "/receipts_delta_GBR_14_10_2017.csv")
        .withcolumn('inst_dt', current_date())


    ol_txn_df.show(5, False)
    ol_txn_df.write.parquet('s3://' +s3_bucket + '/folder')

# read data from mongodb, create data frame out of it and write it into aws s3 bucket
    students = spark\
        .read\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option("database", app_conf["mongodb_config"]["database"])\
        .option("collection", app_conf["mongodb_config"]["collection"])\
        .load()
        .withcolumn('inst_dt', current_date())

    students.show()
    student.write.parquet('s3://' +s3_bucket = '/folder')

#read data from redshift, create data frame out of it and write it into aws s3 bucket
print("Reading txn_fact table ingestion AWS Redshift and creating Dataframe,")

    jdbc_url = ut.get_redshift_jdbc_url(app_secret)
    print(jdbc_url)
    red_df = spark.read\
        .format("io.github.spark_redshift_community.spark.redshift")\
        .option("url", jdbc_url) \
        .option("query", app_conf["redshift_conf"]["query"]) \
        .option("forward_spark_s3_credentials", "true")\
        .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp")\
        .load()
        .withcolumn('inst_dt', current_date())

    red_df.show(5, False)
    red_df.write.parquet('s3://' +s3_bucket = '/folder')


# read data from s3, create data frame out of it and write it into aws s3 bucket
