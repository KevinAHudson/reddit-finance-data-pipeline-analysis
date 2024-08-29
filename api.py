from pymongo import MongoClient
try:
    client = MongoClient('mongodb://localhost:27017/')
    client['reddit_finance_db']['posts'].drop()
except Exception as e:
    print(e)