from kafka import KafkaConsumer
import json
from mongodb_handler import mongodb_handler

bootstrap_servers = ['localhost:9092']
topic_name = 'reddit_data'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='reddit_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Waiting for messages...")

def process_batch(batch):
    for message in batch:
        post_data = message
        result = mongodb_handler.insert_or_update_post(post_data)
        
        post = post_data['post']
        comments = post_data['comments']
        
        if isinstance(result, dict):  # This is the result of an upsert operation
            if result.get('upserted'):
                print(f"Inserted new post to MongoDB: {post['title']} (ID: {post['id']})")
            elif result.get('modified_count', 0) > 0:
                print(f"Updated post in MongoDB: {post['title']} (ID: {post['id']})")
            else:
                print(f"Post already exists in MongoDB (no changes): {post['title']} (ID: {post['id']})")
        else:  # This is the result of an insert operation
            print(f"Inserted new post to MongoDB: {post['title']} (ID: {post['id']})")
        
        print(f"Author: {post['author']}")
        print(f"Score: {post['score']}")
        print(f"Tickers mentioned: {post['tickers']}")
        print(f"Sentiment: {post['sentiment']}")
        print(f"Number of comments: {len(comments)}")
        print("\n---\n")

    print(f"Processed batch of {len(batch)} posts")

batch = []
for message in consumer:
    batch.append(message.value)
    if len(batch) == 20:
        process_batch(batch)
        batch = []
        print("Waiting for next batch of 20 posts...")

# Process any remaining posts
if batch:
    process_batch(batch)