import json
import requests
import pandas as pd
from datetime import datetime, timedelta


URL = 'https://mastodon.social/api/v1/timelines/public'
# URL = 'https://mozilla.social/api/v1/timelines/public'
params = {
    'limit': 20,
    'max_id': None # Set if we want to continue from previous run
}

#since = pd.Timestamp('now', tz='utc') - pd.DateOffset(hour=10)

DAYS_TOTAL = 2
# LIMIT_PER_DAY = 1 # if set we sample per day.
START_DT = datetime.utcnow()
SINCE_DT = (datetime.utcnow() - timedelta(days=DAYS_TOTAL))
should_dump = False



def dump_current_results(dfs):
    print(toots_dfs[0])
    toot_day_df = pd.concat(dfs)
    min_time = toot_day_df['created_at'].min()
    toot_day_df.to_parquet(f'data/toots_mastodon_social_{min_time}.parquet')


since_day_ts = (START_DT -  timedelta(days=DAYS_TOTAL)).timestamp()
toots_dfs = []
while True:
    r = requests.get(URL, params=params)
    try:
        toots = json.loads(r.text)
    except ValueError:
        print('skipped some toots due to parsing error')
        continue

    if toots is None or len(toots) == 0:
        print("No more toots found")
        break

    toot_df = pd.DataFrame(toots)
    null_ts = toot_df.loc[toot_df['created_at'].isnull()]

    if len(null_ts) > 0:
        print(f"Found {len(null_ts)} invalid timestamps")
        should_dump = True

    toots_dfs.append(toot_df)
    toot_df['created_at_ts'] = toot_df['created_at'].apply(lambda x: pd.Timestamp(x).timestamp())
    min_time = toot_df['created_at_ts'].min()
    if min_time <= since_day_ts:
        should_dump = True

    if should_dump:
        dump_current_results(toots_dfs)
        toots_dfs = []
        since_day_ts = min_time
        should_dump = False

    if min_time <= SINCE_DT.timestamp():
        is_end = True
        break
    
    max_id = toots[-1]['id']
    params['max_id'] = max_id
    # if len(toots_dfs) % 1000 == 0:
    print(f"len(results) at time = {len(toots_dfs)} at {datetime.fromtimestamp(min_time)} {max_id}")
    

