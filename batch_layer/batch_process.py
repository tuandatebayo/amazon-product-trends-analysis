from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace, split
from pyspark.sql.types import DoubleType, IntegerType

def preprocess_data(spark_df):
    spark_df = spark_df.filter(col("rating_count").isNotNull())
    spark_df = spark_df.withColumn("discount_percentage", regexp_replace(col("discount_percentage"), "[^0-9]", "").cast(IntegerType()))
    spark_df = spark_df.withColumn("discounted_price", regexp_replace(col("discounted_price"), "[^0-9.]", "").cast(DoubleType()))
    spark_df = spark_df.withColumn("actual_price", regexp_replace(col("actual_price"), "[^0-9.]", "").cast(DoubleType()))
    spark_df = spark_df.withColumn("rating", col("rating").cast(DoubleType()))
    spark_df = spark_df.withColumn("rating_count", col("rating_count").cast(IntegerType()))
    category_split = split(col("category"), "\\|")
    spark_df = spark_df.withColumn("main_category", category_split.getItem(0))
    spark_df = spark_df.withColumn("sub_category", category_split.getItem(1))
    columns_to_drop = ["product_link", "img_link", "review_content", "review_title", "review_id", "user_name", "user_id", "about_product", "category"]
    spark_df = spark_df.drop(*columns_to_drop)
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

def process_cassandra_data():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("CassandraToCassandra") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.cassandra.connection.host", "localhost") \
        .getOrCreate()

    # Read raw data from Cassandra
    raw_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="raw_product_reviews", keyspace="raw_data_streams") \
        .load()

    # Preprocess the data
    processed_df = preprocess_data(raw_df)

    # Write processed data to another Cassandra table
    processed_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "spark_streams") \
        .option("table", "product_reviews_batch") \
        .mode("append") \
        .save()

if __name__ == "__main__":
    process_cassandra_data()
