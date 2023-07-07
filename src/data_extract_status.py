# For all the users in the download file, get their followers
import glob
import json
from time import sleep, time

import pandas as pd
import requests

from src.utils import RateLimiter

# Following status urls


params = {
}

mastodon_host = "https://mastodon.social"
reblogged = "/api/v1/statuses/{}/reblogged_by"
favorited = "/api/v1/statuses/{}/favourited_by"
replies = "/api/v1/statuses/{}/context"

def dump_current_results(dfs):
    if len(dfs) <= 0:
        return
    status_df = pd.concat(dfs).reset_index(drop=True)
    first = status_df['id'].min()
    status_df.to_parquet(f'data/status_mastodon_social_{first}.parquet')


rate_limiter = RateLimiter(300 / (60 * 5))  # 300 request in 5 minutes

MAX_STATUS_DUMP = 100
status_dfs = []
while True:
    # Open up all mastodon toots
    toots_files = glob.glob("data/toots_mastodon*.parquet")
    toots_df = pd.concat([pd.read_parquet(file_p) for file_p in toots_files], axis=0)
    toots_reply = toots_df.loc[toots_df['replies_count'] > 0]
    status_ids_found = toots_reply['id'].to_list()

    status_files = glob.glob("data/status_mastodon*.parquet")

    if status_files:
        status_extracted_dfs = pd.concat([pd.read_parquet(file) for file in status_files], axis=0)
        status_ids_extracted = status_extracted_dfs['parent_reply_id'].to_list()
    else:
        status_ids_extracted = []

    status_ids_to_grab = set(status_ids_found) - set(status_ids_extracted)
    print("Grabbing {} from {} found".format(len(status_ids_to_grab), len(status_ids_found)))
    # Open all status
    for _id in status_ids_to_grab:
        rate_limiter.wait()
        # reblogged = requests.get(reblogged.format(_id), params=params)
        # favorited = requests.get(favorited.format(_id), params=params)
        rsp = requests.get(f'{mastodon_host}{replies.format(_id)}', params=params)

        try:
            status = json.loads(rsp.text)
        except ValueError:
            print('skipped some status {} due to parsing error'.format(_id))
            continue

        if status is None or len(status) == 0 or 'error' in status:
            print("No more status found {}, likely ratelimit, going to wait and retry in 1 minutes".format(status))
            dump_current_results(status_dfs)
            status_dfs = []
            sleep(60)
            continue

        status_df = pd.DataFrame(status['descendants'])
        try:
            status_df['created_at_ts'] = status_df['created_at'].apply(lambda x: pd.Timestamp(x).timestamp())
            status_df['parent_reply_id'] = _id
            status_df['parent_account_id'] = status_df.iloc[0]['in_reply_to_account_id']
        except Exception as e:
            print(e)
            print(status_df)
            continue

        status_dfs.append(status_df)
        if len(status_dfs) >= MAX_STATUS_DUMP:
            print(f"Dumped: {len(status_dfs)}")
            dump_current_results(status_dfs)
            status_dfs = []
