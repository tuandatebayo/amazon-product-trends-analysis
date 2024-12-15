import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from datasets import load_dataset
import random

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

dataset = load_dataset("tuandatebayo/amazon_sale", split="train", trust_remote_code=True)

def get_data():
    import requests

    random_index = random.randint(0, len(dataset) - 1)
    return dataset[random_index]

def format_data(res):
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
    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time() 

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            res = get_data()
            res = format_data(res)

            producer.send('amazon_reviews', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )


# stream_data()