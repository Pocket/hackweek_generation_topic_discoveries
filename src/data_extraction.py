import json
from datetime import datetime, timedelta
from time import sleep, time

import pandas as pd
import requests

URL = 'https://mastodon.social/api/v1/timelines/public'
# URL = 'https://mozilla.social/api/v1/timelines/public'
params = {
    'limit': 40,
    'max_id': '110613808387143738'  # Set if we want to continue from previous run
}

# since = pd.Timestamp('now', tz='utc') - pd.DateOffset(hour=10)

DAYS_TOTAL = 90
LIMIT_PER_DAY = None  # if set we sample per day.
START_DT = datetime.utcnow()
SINCE_DT = (datetime.utcnow() - timedelta(days=DAYS_TOTAL))
should_dump = False


class RateLimiter:
    def __init__(self, min_update_interval_seconds: float) -> None:
        self._min_update_interval_seconds = min_update_interval_seconds
        self._last_update: float = time()

    def wait(self) -> float:
        now = time()
        delta = now - self._last_update
        wait = max(self._min_update_interval_seconds - delta, 0)
        if wait != 0:
            sleep(wait)
        self._last_update = now
        return delta - self._min_update_interval_seconds

    def reset(self) -> None:
        self._last_update = time()


def dump_current_results(dfs):
    if len(dfs) <= 0:
        return

    toot_day_df = pd.concat(dfs)
    min_time = toot_day_df['created_at'].min()
    toot_day_df.to_parquet(f'data/toots_mastodon_social_{min_time}.parquet')


since_day_ts = (START_DT - timedelta(days=DAYS_TOTAL)).timestamp()
toots_dfs = []
rate_limiter = RateLimiter(300 / (60 * 5))  # 300 request in 5 minutes
while True:
    rate_limiter.wait()
    r = requests.get(URL, params=params)
    try:
        toots = json.loads(r.text)
    except ValueError:
        print('skipped some toots due to parsing error')
        continue

    if toots is None or len(toots) == 0 or 'error' in toots:
        print("No more toots found {}, likely ratelimit, going to wait and retry in 1 minutes".format(toots))
        dump_current_results(toots_dfs)
        toots_dfs = []
        sleep(60)
        continue

    toot_df = pd.DataFrame(toots)
    null_ts = toot_df.loc[toot_df['created_at'].isnull()]

    if len(null_ts) > 0:
        print(f"Found {len(null_ts)} invalid timestamps")
        should_dump = True

    toots_dfs.append(toot_df)
    toot_df['created_at_ts'] = toot_df['created_at'].apply(lambda x: pd.Timestamp(x).timestamp())
    med_time = toot_df['created_at_ts'].median()
    if med_time <= since_day_ts or (LIMIT_PER_DAY and LIMIT_PER_DAY > len(toots_dfs)):
        should_dump = True

    if should_dump:
        dump_current_results(toots_dfs)
        toots_dfs = []
        since_day_ts = med_time
        should_dump = False

    if med_time <= SINCE_DT.timestamp() or (LIMIT_PER_DAY and LIMIT_PER_DAY > len(toots_dfs)):
        break

    max_id = toots[-1]['id']
    params['max_id'] = max_id
    # if len(toots_dfs) % 1000 == 0:
    print(f"Results: {len(toot_df)} at: {datetime.fromtimestamp(med_time)} maxId: {max_id}")
