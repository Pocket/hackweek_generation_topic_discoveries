## Extract users data from the directory

# mastodon has this API to search for users (directory)
import requests
import json
import pandas as pd


response_data = []
low = 0
high = 40
offset = 40000

def extract_users_from_directory():
    for i in range(low, high, offset):
        response = requests.get(f"https://mastodon.social/api/v1/directory?offset={i}&limit={i+40}")
        response_data.append(json.loads(response.text))
        if i %1000 == 0:
            print(f"completed extracting {i} users from the directory")
    print(len(response_data))

    directory_df = pd.concat([pd.DataFrame(block) for block in response_data], axis=0)
    directory_df.to_parquet("data/directory_10k_users.parquet")

if __name__ == '__main__':
    extract_users_from_directory()