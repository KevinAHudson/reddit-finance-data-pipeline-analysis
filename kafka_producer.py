from kafka import KafkaProducer
import json
import time
from reddit_producer import scrape_subreddit, find_subreddit_threads
import random
# Kafka configuration
bootstrap_servers = ['localhost:9092']
topic_name = 'reddit_data'

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_batch(batch):
    for post in batch:
        producer.send(topic_name, post)
        print(f"Sent: {post['post']['title']}")
    producer.flush()
    print(f"Batch of {len(batch)} posts sent. Waiting for consumer...")
    time.sleep(10)  # Wait for 10 seconds to allow consumer to process

def send_reddit_data():
    while True:
        subreddit_name = 'wallstreetbets'
        post_permalinks = find_subreddit_threads(subreddit_name)

        batch = []
        post_count = 0
        total_comments = 0

        for permalink in post_permalinks:
            try:
                post_data = scrape_subreddit(permalink)
                if post_data:
                    batch.append(post_data)
                    post_count += 1
                    comments_count = len(post_data['comments'])
                    total_comments += comments_count
                    print(f"Saved data for post: {post_data['post']['title']}")
                    print(f"Total posts saved: {post_count}")
                    print(f"Total comments saved: {total_comments}")

                    if len(batch) == 20:
                        send_batch(batch)
                        batch = []

            except Exception as e:
                print(f"Error scraping post {permalink}: {str(e)}")
            finally:
                time.sleep(random.uniform(1, 3))  # Random delay between requests

        # Send any remaining posts
        if batch:
            send_batch(batch)

        print(f"Scraped and sent {post_count} posts")
        time.sleep(300)  # Wait for 5 minutes before scraping again

if __name__ == "__main__":
    send_reddit_data()