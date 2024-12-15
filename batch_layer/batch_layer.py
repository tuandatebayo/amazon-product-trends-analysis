from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement
import json

# Configure Kafka Consumer
consumer = KafkaConsumer(
    'amazon_reviews',  # Kafka topic
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='cassandra_batch_consumer'
)

# Configure Cassandra
cluster = Cluster(['localhost'])
session = cluster.connect()

# Create a new keyspace and table for raw Kafka messages
def create_keyspace_and_table():
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS raw_data_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    session.set_keyspace('raw_data_streams')
    session.execute("""
        CREATE TABLE IF NOT EXISTS raw_product_reviews (
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
    print("Keyspace and table created successfully!")

# Function to store Kafka messages in Cassandra
def store_data_in_cassandra():
    batch = BatchStatement()
    insert_query = session.prepare("""
        INSERT INTO raw_product_reviews (
            product_id, product_name, category, discounted_price, 
            actual_price, discount_percentage, rating, rating_count,
            about_product, user_id, user_name, review_id, review_title,
            review_content, img_link, product_link
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    batch_size = 5  # Number of records per batch
    batch_counter = 0

    for message in consumer:
        res = json.loads(message.value.decode('utf-8'))

        # Extract data into a dictionary with default values for missing fields
        data = {
            "product_id": res.get('product_id', 'N/A'),
            "product_name": res.get('product_name', 'N/A'),
            "category": res.get('category', 'N/A'),
            "discounted_price": res.get('discounted_price', 'N/A'),
            "actual_price": res.get('actual_price', 'N/A'),
            "discount_percentage": res.get('discount_percentage', 'N/A'),
            "rating": res.get('rating', 'N/A'),
            "rating_count": res.get('rating_count', 'N/A'),
            "about_product": res.get('about_product', 'N/A'),
            "user_id": res.get('user_id', 'N/A'),
            "user_name": res.get('user_name', 'N/A'),
            "review_id": res.get('review_id', 'N/A'),
            "review_title": res.get('review_title', 'N/A'),
            "review_content": res.get('review_content', 'N/A'),
            "img_link": res.get('img_link', 'N/A'),
            "product_link": res.get('product_link', 'N/A'),
        }

        # Add the data to the batch
        batch.add(insert_query, (
            data["product_id"], data["product_name"], data["category"], data["discounted_price"],
            data["actual_price"], data["discount_percentage"], data["rating"], data["rating_count"],
            data["about_product"], data["user_id"], data["user_name"], data["review_id"],
            data["review_title"], data["review_content"], data["img_link"], data["product_link"]
        ))
        batch_counter += 1

        # Execute the batch when the batch size is reached
        if batch_counter >= batch_size:
            session.execute(batch)
            print(f"Inserted {batch_counter} records into Cassandra.")
            batch = BatchStatement()  # Reset the batch
            batch_counter = 0

    # Handle any remaining records in the batch
    if batch_counter > 0:
        session.execute(batch)
        print(f"Inserted {batch_counter} remaining records into Cassandra.")

if __name__ == "__main__":
    create_keyspace_and_table()
    store_data_in_cassandra()
