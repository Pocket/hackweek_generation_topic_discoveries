# This depends on the output from users_directory.py
# This program uses the statuses related to the extracted from the directory.
# A sing user could have interacted with many twoot posts (one to many).
# We right not put a threshold of maximum of 100 statuses per user for demo.

## Extract users data from the directory

# mastodon has this API to search for users (directory)
import requests
import json
import pandas as pd


response_data = []
low = 0
high = 100
offset = 20

def fetch_list_of_users():
    users_list = pd.read_parquet('data/directory_10k_users.parquet')['id'].unique()
    print(f"Unique number of users = {len(users_list)}")
    return users_list

def extract_users_feeds(users_list):
    response_content_data = []
    for i, user_id in enumerate(users_list):
        if i % 10 == 0:
            print(f"completed fetching statuses of {i} users")
            print(f"total records fetched = {len(response_content_data)}")
        
        for i in range(low, high, offset):
            response = requests.get(f"https://mastodon.social/api/v1/accounts/{user_id}/statuses?offset={i}&limit={i+offset}")
            json_data = response.json()
            try:
                response_content_data.append(pd.DataFrame.from_dict(json_data))
            except ValueError:
                continue
    user_feeds_df = pd.concat(response_content_data, axis=0)
    user_feeds_df['user_id'] = user_feeds_df['account'].apply(lambda details: details['id'])
    print(len(user_feeds_df))
    user_feeds_df.to_parquet("data/2023-06-27_users_feeds_df_1k.parquet")

if __name__ == '__main__':
    users_list = fetch_list_of_users()
    extract_users_feeds(users_list[:1000])