from pymongo import MongoClient, DESCENDING
from pymongo.errors import DuplicateKeyError
from collections import Counter

class MongoDBHandler:
    def __init__(self, connection_string='mongodb://localhost:27017/'):
        self.client = MongoClient(connection_string)
        self.db = self.client['reddit_finance_db']
        self.posts_collection = self.db['posts']
        
        # Create a unique index on post ID
        self.posts_collection.create_index([("post.id", DESCENDING)], unique=True)
        self.posts_collection.create_index([
            ("post.created_utc", -1),
            ("post.score", -1),
            ("post.tickers", 1)
        ])
        self.posts_collection.create_index([
            ("post.sentiment.compound", -1),
            ("post.created_utc", -1)
        ])

    def check_duplicate(self, post_id):
        """Check if a post with the given ID already exists in the database."""
        return self.posts_collection.find_one({"post.id": post_id}) is not None

    def insert_post(self, post_data):
        """Insert a post if it doesn't already exist."""
        post_id = post_data['post']['id']
        if self.check_duplicate(post_id):
            print(f"Post {post_id} already exists in the database. Skipping.")
            return None
        try:
            result = self.posts_collection.insert_one(post_data)
            print(f"Inserted post {post_id} into the database.")
            return result
        except DuplicateKeyError:
            print(f"Post {post_id} already exists. Skipping.")
            return None

    def update_post(self, post_data):
        """Update an existing post."""
        post_id = post_data['post']['id']
        result = self.posts_collection.update_one(
            {"post.id": post_id},
            {"$set": post_data}
        )
        if result.modified_count > 0:
            print(f"Updated post {post_id} in the database.")
        else:
            print(f"No changes made to post {post_id}.")
        return result
        
    def insert_or_update_post(self, post_data):
        post_id = post_data['post']['id']
        result = self.posts_collection.update_one(
            {"post.id": post_id},
            {"$set": post_data},
            upsert=True
        )
        return {
            "matched_count": result.matched_count,
            "modified_count": result.modified_count,
            "upserted": result.upserted_id is not None
        }

    def get_recent_posts(self, limit=10):
        return self.posts_collection.find().sort("post.created_utc", -1).limit(limit)

    def print_recent_posts(self, limit=10):
        recent_posts = self.get_recent_posts(limit)
        for post in recent_posts:
            print(f"Title: {post['post']['title']}")
            print(f"Author: {post['post']['author']}")
            print(f"Tickers: {post['post']['tickers']}")
            print(f"ID: {post['post']['id']}")
            print(f"Number of comments: {len(post['comments'])}")
            if post['comments']:
                print(f"First comment: {post['comments'][0]['body'][:100]}...")
            print("---")

    def check_for_duplicates(self):
        """Check for duplicate posts in the database."""
        pipeline = [
            {"$group": {
                "_id": "$post.id",
                "count": {"$sum": 1},
                "titles": {"$push": "$post.title"}
            }},
            {"$match": {
                "count": {"$gt": 1}
            }}
        ]

        duplicates = list(self.posts_collection.aggregate(pipeline))
        
        if duplicates:
            print(f"Found {len(duplicates)} duplicate posts:")
            for dup in duplicates:
                print(f"Post ID: {dup['_id']}")
                print(f"Count: {dup['count']}")
                print(f"Titles: {dup['titles']}")
                print("---")
        else:
            print("No duplicates found in the database.")

        return duplicates

    def get_database_stats(self):
        """Get statistics about the database."""
        post_count = self.posts_collection.count_documents({})
        
        comment_count_result = list(self.posts_collection.aggregate([
            {"$project": {"comment_count": {"$size": "$comments"}}},
            {"$group": {"_id": None, "total_comments": {"$sum": "$comment_count"}}}
        ]))
        
        comment_count = comment_count_result[0]['total_comments'] if comment_count_result else 0
        
        unique_authors = len(self.posts_collection.distinct("post.author"))
        
        tickers_list = self.posts_collection.aggregate([
            {"$unwind": "$post.tickers"},
            {"$group": {"_id": "$post.tickers", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ])
        
        top_tickers = list(tickers_list)

        return {
            "total_posts": post_count,
            "total_comments": comment_count,
            "avg_comments_per_post": comment_count / post_count if post_count > 0 else 0,
            "unique_authors": unique_authors,
            "top_tickers": top_tickers
        }

    def print_database_stats(self):
        stats = self.get_database_stats()
        print(f"Database Statistics:")
        print(f"Total Posts: {stats['total_posts']}")
        print(f"Total Comments: {stats['total_comments']}")
        print(f"Average Comments per Post: {stats['avg_comments_per_post']:.2f}")
        print(f"Unique Authors: {stats['unique_authors']}")
        print("Top 10 Tickers:")
        for ticker in stats['top_tickers']:
            print(f"  {ticker['_id']}: {ticker['count']}")

mongodb_handler = MongoDBHandler()

def check_database():
    try:
        print("Checking database for duplicates:")
        mongodb_handler.check_for_duplicates()
        print("\nDatabase statistics:")
        mongodb_handler.print_database_stats()
    except Exception as e:
        print(f"An error occurred while checking the database: {str(e)}")

if __name__ == "__main__":
    check_database()