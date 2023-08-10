import os,json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.conf import SparkConf
from pyspark.sql.types import *
from delta import *
from dotenv import load_dotenv


load_dotenv()
appname=os.environ["Spark_appname"]
loglevel=os.environ["Spark_loglevel"]
kafka_bootstrap_server=os.environ["Kafka_bootstrap_server"]
kafka_topic=os.environ["Kafka_topic"]
spark_readStream_options = json.loads(os.environ["Spark_readStream_options"])
sink_type = os.environ["Spark_sink_type"]
output_mode = os.environ["Spark_output_mode"]
sink_location = os.environ["Spark_sink_location"]
checkpointLocation = os.environ["Spark_checkpointLocation"]
s3_accessKey = os.environ["s3_accessKey"]
s3_secretKey = os.environ["s3_secretKey"]
s3_endpoint = os.environ["s3_endpoint"]

packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0","org.apache.hadoop:hadoop-aws:3.3.2",
    "org.apache.hadoop:hadoop-common:3.3.2","com.amazonaws:aws-java-sdk-bundle:1.12.523"
]

def create_session() -> SparkSession:
    builder = SparkSession \
        .builder.appName(appname) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.access.key", s3_accessKey) \
        .config("spark.hadoop.fs.s3a.secret.key", s3_secretKey) \
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
        .config("spark.ui.prometheus.enabled",True)

    session = configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()
    session.sparkContext.setLogLevel(loglevel)
    return session

def kafka_consumer(session):
        inputDF = session.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
            .option("subscribe", kafka_topic) \
            .options(**spark_readStream_options) \
            .load() \
            .selectExpr("CAST(value AS STRING)", "CAST(partition AS INT )", "CAST(offset AS LONG)")
        return inputDF

def process(df):
    schema = StructType([
                StructField("device_id", StringType(), True),
                StructField("temperature", IntegerType(), True),
                StructField("location", StringType(), True),
                StructField("time", StringType(), True)
            ])
    df = df.select(from_json(df.value, schema).alias("data"), "partition", "offset") \
        .select("data.*","partition","offset")
    df = df.withColumn("RECORD_DATE", from_unixtime(col("time")))
    return df

def sink(df: DataFrame):
    if sink_type == 'console':
        query = df.writeStream \
            .format("console") \
            .outputMode(output_mode) \
            .start()

        query.awaitTermination()

    if sink_type == 'delta':
        query = df.writeStream \
            .format("delta") \
            .outputMode(output_mode) \
            .option("checkpointLocation", checkpointLocation) \
            .start(sink_location)


    return query