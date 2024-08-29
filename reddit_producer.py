import requests
import json
import time
import re
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import random
from ratelimit import limits, sleep_and_retry

nltk.download('vader_lexicon')
sia = SentimentIntensityAnalyzer()

CALLS = 45
RATE_LIMIT = 45

@sleep_and_retry
@limits(calls=CALLS, period=RATE_LIMIT)
def make_api_call(url, headers, max_retries=3):
    retries = 0
    while retries < max_retries:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response
        elif response.status_code == 429:
            wait_time = int(response.headers.get('Retry-After', 60))
            print(f"Rate limited. Waiting for {wait_time} seconds.")
            time.sleep(wait_time + random.uniform(1, 5))  # Add some randomness
            retries += 1
        else:
            print(f"Unexpected status code: {response.status_code}")
            return None
    print("Max retries reached. Couldn't complete the request.")
    return None

def extract_tickers(text):
    # Define a regular expression pattern for tickers
    common_false_positives = {'A', 'I', 'AM', 'AN', 'AS', 'AT', 'BE', 'BY', 'DO', 'GO', 'IF', 'IN', 'IS', 'IT', 'NO', 'OF', 'ON', 'OR', 'SO', 'TO', 'UP', 'US', 'WE'}
    
    ticker_pattern = r'\b[A-Z]{1,5}\b'
    tickers = re.findall(ticker_pattern, text)
    #if ticker is in common_false_positives dont return the ticker
    tickers = [ticker for ticker in tickers if ticker not in common_false_positives]
    return tickers


def analyze_sentiment(text):
    sentiment = sia.polarity_scores(text)
    return sentiment


def find_subreddit_threads(sub):
    url = f'https://www.reddit.com/r/{sub}/new/.json?limit=500'
    headers = {'User-Agent': 'Learning'}

    response = make_api_call(url,headers)
    if response.status_code == 200:
        data = response.json()

        post_links = []
        for post in data['data']['children']:
            permalink = post['data']['permalink']
            post_links.append(permalink)

        return post_links

    else:
        print("Failed to fetch data:", response.status_code)
        return []


def scrape_subreddit(permalink):
    url = f'https://www.reddit.com{permalink}.json?limit=500'
    headers = {'User-Agent': 'Learning'}

    response = make_api_call(url,headers)   
    if response.status_code == 200:
        post_data = response.json()
        post = post_data[0]['data']['children'][0]['data']
        comments_data = post_data[1]['data']['children']
        post_text = post['title'] + ' ' + post.get('selftext', '')
        tickers = extract_tickers(post_text)
        sentiment = analyze_sentiment(post_text)
        post_info = {
            'id': post['id'],
            'title': post['title'],
            'author': post['author'],
            'score': post['score'],
            'permalink': post['permalink'],
            'created_utc': post['created_utc'],
            'selftext': post['selftext'],
            'num_comments': post['num_comments'],
            'tickers': tickers,
            'sentiment': sentiment
        }
        comments = []
        for comment in comments_data:
            if comment['kind'] == 't1':
                comment_data = comment['data']
                if comment_data['author'] == 'VisualMod':
                    continue
                comment_text = comment_data['body']
                tickers = extract_tickers(comment_text)
                sentiment = analyze_sentiment(comment_text)
                comments.append({
                    'id': comment_data['id'],
                    'author': comment_data['author'],
                    'score': comment_data['score'],
                    'body': comment_data['body'],
                    'created_utc': comment_data['created_utc'],
                    'tickers': tickers,
                    'sentiment': sentiment
                })
        post_dict = {
            'post': post_info,
            'comments': comments
        }
        return post_dict
    else:
        print("Failed to fetch data:", response.status_code)
        return None


def main():
    subreddit_name = 'wallstreetbets'
    post_permalinks = find_subreddit_threads(subreddit_name)

    all_data = []
    total_comments = 0
    print(len(post_permalinks))
    for permalink in post_permalinks:
        try:
            post_data = scrape_subreddit(permalink)
            if post_data:
                all_data.append(post_data)
                comments_count = len(post_data['comments'])
                total_comments += comments_count
                print(f"Saved data for post: {post_data['post']['title']}")
                print(f"Total posts saved: {len(all_data)}")
                print(f"Total comments saved: {total_comments}")
        except Exception as e:
            print(f"Error scraping post {permalink}: {str(e)}")
        finally:
            # Add a random delay between requests
            time.sleep(random.uniform(1.5, 5))
    
    print(f"Scraped {len(all_data)} posts")
    return all_data

if __name__ == "__main__":
    main()