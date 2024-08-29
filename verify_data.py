from mongodb_handler import MongoDBHandler

def verify_data_integrity():
    handler = MongoDBHandler()
    print("Connected to MongoDB")
    
    posts = list(handler.get_recent_posts(limit=100))
    print(f"Retrieved {len(posts)} posts from MongoDB")
    
    if len(posts) == 0:
        print("No posts found in the database. Please ensure that:")
        print("1. Your Kafka producer and consumer have run successfully")
        print("2. Data has been inserted into MongoDB")
        print("3. The MongoDB connection details in MongoDBHandler are correct")
        return

    for post in posts:
        # Check if all required fields are present
        required_fields = ['id', 'title', 'author', 'score', 'created_utc', 'tickers', 'sentiment']
        for field in required_fields:
            assert field in post['post'], f"Missing field '{field}' in post {post['post']['id']}"
        
        # Check if sentiment is correctly formatted
        assert all(key in post['post']['sentiment'] for key in ['neg', 'neu', 'pos', 'compound']), f"Incorrect sentiment format in post {post['post']['id']}"
        
        # Check comments
        for comment in post['comments']:
            assert all(field in comment for field in ['id', 'author', 'body', 'created_utc', 'tickers', 'sentiment']), f"Missing field in comment {comment['id']} of post {post['post']['id']}"
    
    print(f"Verified {len(posts)} posts successfully!")

if __name__ == "__main__":
    verify_data_integrity()