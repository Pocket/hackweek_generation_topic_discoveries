# hackweek_generation_topic_discoveries
hackweek_generation_topic_discoveries


Steps to create 1 week worth of toots from Mastodon
```
 1. mkdir data
 2. python src/data_extraction.py
 3. python -m spacy download en_core_web_sm
 4. python src/toots_sentiment_topics.py
 5. streamlit run src/app.py
```