import tweepy
from elasticsearch import Elasticsearch, helpers
import json
import os

# Load Twitter API credentials from environment variables
API_KEY = os.getenv("TWITTER_API_KEY")
API_SECRET_KEY = os.getenv("TWITTER_API_SECRET_KEY")
ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")
BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")

# Initialize Tweepy client with Bearer Token
client = tweepy.Client(bearer_token=BEARER_TOKEN)

# Initialize Elasticsearch client (assuming it's running locally on http://localhost:9200)
es = Elasticsearch("http://localhost:9200")

# Define the index name
INDEX_NAME = "long_covid_tweets"

# Define the search query for long COVID-related content with symptoms and demographic characteristics
query = """
long covid (symptoms OR fatigue OR breathlessness OR headache OR 'brain fog' OR dizziness OR loss OR smell OR taste) 
OR post-covid OR 'long haul covid' 
OR (long covid AND (age OR children OR elderly OR gender OR male OR female))
"""

# Define the fields to retrieve from the Twitter API
fields = ['created_at', 'text', 'author_id', 'lang', 'public_metrics']

# Function to search tweets using the Twitter API v2
def search_tweets(query, max_results=10):
    try:
        # Make a request to the Twitter API to search for recent tweets matching the provided query
        response = client.search_recent_tweets(query=query, tweet_fields=fields, max_results=max_results)
        tweets = response.data

        if not tweets:
            print("No tweets found matching the query.")
            return []

        # Convert tweets to Elasticsearch bulk actions format
        actions = [
            {
                "_index": INDEX_NAME,
                "_source": {
                    "tweet_id": tweet.id,
                    "text": tweet.text,
                    "author_id": tweet.author_id,
                    "created_at": tweet.created_at,
                    "lang": tweet.lang,
                    "public_metrics": tweet.public_metrics
                }
            }
            for tweet in tweets
        ]

        return actions

    except Exception as e:
        print(f"Error occurred while searching tweets: {e}")
        return []

# Function to store tweets in Elasticsearch
def store_tweets_in_elasticsearch(actions):
    try:
        if actions:
            # Bulk insert the tweets into the Elasticsearch index
            helpers.bulk(es, actions)
            print(f"Successfully inserted {len(actions)} tweets into Elasticsearch.")
        else:
            print("No tweets to insert.")
    except Exception as e:
        print(f"Error occurred while inserting into Elasticsearch: {e}")

# Function to create the Elasticsearch index if it doesn't exist
def create_index_if_not_exists():
    if not es.indices.exists(index=INDEX_NAME):
        # Create the index if it does not exist
        es.indices.create(index=INDEX_NAME)
        print(f"Index {INDEX_NAME} created.")
    else:
        print(f"Index {INDEX_NAME} already exists.")

if __name__ == "__main__":
    # Ensure the Elasticsearch index is created
    create_index_if_not_exists()

    # Search for tweets matching the query
    actions = search_tweets(query=query, max_results=10)

    # Store the retrieved tweets in Elasticsearch
    store_tweets_in_elasticsearch(actions)
