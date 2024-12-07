import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    logging.info("Keyspace created successfully!")


def create_table(session):
    # session.execute("""
    # CREATE TABLE IF NOT EXISTS spark_streams.product_reviews (
    #     id UUID PRIMARY KEY,
    #     main_category TEXT,
    #     title TEXT,
    #     average_rating TEXT,
    #     rating_number TEXT,
    #     price TEXT,
    #     categories TEXT,
    #     parent_asin TEXT,
    #     timestamp TEXT
    # );
    # """)
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.product_reviews (
        product_id TEXT PRIMARY KEY,
        product_name TEXT,
        category TEXT,
        discounted_price TEXT,
        actual_price TEXT,
        discount_percentage TEXT,
        rating TEXT,
        rating_count TEXT,
        about_product TEXT,
        user_id TEXT,
        user_name TEXT,
        review_id TEXT,
        review_title TEXT,
        review_content TEXT,
        img_link TEXT,
        product_link TEXT
    );
    """)
    logging.info("Table created successfully!")


def create_spark_connection():
    try:
        spark_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return spark_conn
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to exception: {e}")
        return None


def connect_to_kafka(spark_conn):
    try:
        kafka_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'amazon_reviews') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
        return kafka_df
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")
        return None


def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to: {e}")
        return None


def create_selection_df_from_kafka(kafka_df):
    # schema = StructType([
    #     StructField("id", StringType(), False),
    #     StructField("main_category", StringType(), True),
    #     StructField("title", StringType(), True),
    #     StructField("average_rating", StringType(), True),
    #     StructField("rating_number", StringType(), True),
    #     StructField("price", StringType(), True),
    #     StructField("categories", StringType(), True),
    #     StructField("parent_asin", StringType(), True),
    #     StructField("timestamp", StringType(), False)
    # ])
    schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("discounted_price", StringType(), True),
        StructField("actual_price", StringType(), True),
        StructField("discount_percentage", StringType(), True),
        StructField("rating", StringType(), True),
        StructField("rating_count", StringType(), True),
        StructField("about_product", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("review_id", StringType(), True),
        StructField("review_title", StringType(), True),
        StructField("review_content", StringType(), True),
        StructField("img_link", StringType(), True),
        StructField("product_link", StringType(), True)
    ])
    
    selection_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')) \
        .select("data.*")
    logging.info("Kafka data successfully parsed into structured format")
    return selection_df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Create Spark connection
    spark_conn = create_spark_connection()

    if spark_conn:
        # Connect to Kafka
        kafka_df = connect_to_kafka(spark_conn)
        if kafka_df:
            selection_df = create_selection_df_from_kafka(kafka_df)

            # Connect to Cassandra
            cassandra_session = create_cassandra_connection()
            if cassandra_session:
                create_keyspace(cassandra_session)
                create_table(cassandra_session)

                # Start streaming
                logging.info("Starting streaming process...")
                streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                                   .option('checkpointLocation', '/var/checkpoints/amazon_reviews')
                                   .option('keyspace', 'spark_streams')
                                   .option('table', 'product_reviews')
                                   .start())

                streaming_query.awaitTermination()
