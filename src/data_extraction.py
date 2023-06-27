import json
import requests
import pandas as pd


URL = 'https://mastodon.social/api/v1/timelines/public'
# URL = 'https://mozilla.social/api/v1/timelines/public'
params = {
    'limit': 40
}

#since = pd.Timestamp('now', tz='utc') - pd.DateOffset(hour=10)
since = pd.Timestamp('now', tz='utc') - pd.DateOffset(1)
is_end = False

results = []

while True:
    r = requests.get(URL, params=params)
    try:
        toots = json.loads(r.text)
    except ValueError:
        print('skipped some toots due to parsing error')

    if len(toots) == 0:
        break
    
    for t in toots:
        try: 
            timestamp = pd.Timestamp(t['created_at'], tz='utc')
        except:
            is_end = True
            break
        if timestamp <= since:
            is_end = True
            break
            
        results.append(t)
    
    if is_end:
        break
    
    max_id = toots[-1]['id']
    params['max_id'] = max_id
    if len(results) % 1000 == 0:
        print(f"len(results) = {len(results)}")
    
df = pd.DataFrame(results)
df.to_parquet('data/toots_mastodon_social_last_1_day.parquet')
