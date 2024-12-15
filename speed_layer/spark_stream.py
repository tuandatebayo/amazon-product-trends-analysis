import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, regexp_replace, split
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


# def create_table(session):
#     session.execute("""
#     CREATE TABLE IF NOT EXISTS spark_streams.product_reviews (
#         product_id TEXT PRIMARY KEY,
#         product_name TEXT,
#         category TEXT,
#         discounted_price TEXT,
#         actual_price TEXT,
#         discount_percentage TEXT,
#         rating TEXT,
#         rating_count TEXT,
#         about_product TEXT,
#         user_id TEXT,
#         user_name TEXT,
#         review_id TEXT,
#         review_title TEXT,
#         review_content TEXT,
#         img_link TEXT,
#         product_link TEXT
#     );
#     """)
#     print("Table created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.product_reviews (
        product_id TEXT PRIMARY KEY,
        product_name TEXT,
        discounted_price DOUBLE,
        actual_price DOUBLE,
        discount_percentage INT,
        rating DOUBLE,
        rating_count INT,
        main_category TEXT,
        sub_category TEXT,
        rating_score TEXT
    );
    """)
    print("Table created successfully!")

def create_batch_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.product_reviews_batch (
        product_id TEXT PRIMARY KEY,
        product_name TEXT,
        discounted_price DOUBLE,
        actual_price DOUBLE,
        discount_percentage INT,
        rating DOUBLE,
        rating_count INT,
        main_category TEXT,
        sub_category TEXT,
        rating_score TEXT,
        processed_timestamp TIMESTAMP
    );
    """)
    print("Batch table created successfully!")

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'amazon_reviews') \
            .option('startingOffsets', 'latest') \
            .option('failOnDataLoss', 'false') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
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
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

def preprocess_data(spark_df):
    # 1. Check for missing values and filter out rows with missing rating_count
    spark_df = spark_df.filter(col("rating_count").isNotNull())

    # 2. Remove % in discount_percentage and convert it to integer

    spark_df = spark_df.withColumn("discount_percentage", regexp_replace(col("discount_percentage"), "[^0-9]", "").cast(IntegerType()))

    # 3. Convert data types and clean up currency symbols in price columns
    spark_df = spark_df.withColumn("discounted_price", regexp_replace(col("discounted_price"), "[^0-9.]", "").cast(DoubleType()))
    spark_df = spark_df.withColumn("actual_price", regexp_replace(col("actual_price"), "[^0-9.]", "").cast(DoubleType()))
    spark_df = spark_df.withColumn("rating", col("rating").cast(DoubleType()))
    spark_df = spark_df.withColumn("rating_count", col("rating_count").cast(IntegerType()))

    # 4. Separate the category column into main and subcategories
    category_split = split(col("category"), "\\|")
    spark_df = spark_df.withColumn("main_category", category_split.getItem(0))
    spark_df = spark_df.withColumn("sub_category", category_split.getItem(1))

    # 5. Drop unnecessary columns
    columns_to_drop = ["product_link", "img_link", "review_content", "review_title", "review_id", "user_name", "user_id", "about_product", "category"]
    spark_df = spark_df.drop(*columns_to_drop)

    # 6. Create a new column `rating_score` based on the rating column
    spark_df = spark_df.withColumn(
        "rating_score",
        when(col("rating") < 2.0, "Poor")
        .when((col("rating") >= 2.0) & (col("rating") < 3.0), "Below Average")
        .when((col("rating") >= 3.0) & (col("rating") < 4.0), "Average")
        .when((col("rating") >= 4.0) & (col("rating") < 5.0), "Good")
        .when(col("rating") == 5.0, "Excellent")
        .otherwise("Unknown")
    )

    return spark_df

if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = preprocess_data(create_selection_df_from_kafka(spark_df))
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            create_batch_table(session)

            logging.info("Streaming is being started...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'product_reviews')
                               .start())

            streaming_query.awaitTermination()
