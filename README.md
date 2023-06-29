# hackweek_generation_topic_discoveries
hackweek_generation_topic_discoveries


S3 Bucket with data: s3://ml-team-pocket/social_hackweek_2023/


Steps to create 1 week worth of toots from Mastodon and run streamlit


For Topic Discoveries app:
```
 1. mkdir data
 2. python src/data_extraction.py
 3. python -m spacy download en_core_web_sm
 4. python src/toots_sentiment_topics.py
 5. streamlit run src/app.py
```



For Topic Discoveries app:
```
 1. mkdir data
 2. python src/discovery_data_prep.py
 3. streamlit run src/app_discovery.py
```