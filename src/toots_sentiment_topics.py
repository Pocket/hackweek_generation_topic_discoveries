# This batch program generates sentiments an topics for the extracted toots.
# This reads the output from the data_extraction,py

import pandas as pd
import glob
import nltk
from transformers import pipeline
from utils import clean_html, clean_string
from transformers import AutoModelForSequenceClassification, TFAutoModelForSequenceClassification
from transformers import AutoTokenizer
import numpy as np
from scipy.special import expit
import os

nltk.download('stopwords')
sentiment_pipeline = pipeline("sentiment-analysis")
MODEL = f"cardiffnlp/tweet-topic-21-multi"
tokenizer = AutoTokenizer.from_pretrained(MODEL)

model = AutoModelForSequenceClassification.from_pretrained(MODEL)
class_mapping = model.config.id2label

results_path = 'data/results'

os.makedirs(results_path, exist_ok=True)


def read_toots_data() -> pd.DataFrame:
    """read the extracted toots data"""
    datasets = glob.glob("data/toots_mastodon*.parquet")
    df = pd.concat([pd.read_parquet(data) for data in datasets], axis=0)
    df = df[(df['content'].apply(len) < 256) & (df['language'] == 'en')]
    df = df[~df['content'].isna()].reset_index(drop=True)
    return df #.sample(2000)  ## smaple of 3000 toots

def perform_sentiment_analysis(df) -> pd.DataFrame:
    sent_scores = sentiment_pipeline(df['content_cleaned'].str.lower().values.tolist())
    return pd.concat([df, pd.json_normalize(sent_scores)], axis=1)

def predict_topic(text):
    """Predict the topics using the model (https://huggingface.co/cardiffnlp/tweet-topic-21-multi)
    0: arts_&_culture 	
    1: business_&_entrepreneurs 	
    2: celebrity_&_pop_culture 	
    3: diaries_&_daily_life 	
    4: family 		
    5: fashion_&_style 	
    6: film_tv_&_video 	
    7: fitness_&_health 	
    8: food_&_dining 	
    9: gaming 	
    10: learning_&_educational 	
    11: music 	
    12: news_&_social_concern 	
    13: other_hobbies 	
    14: relationships 
    15: science_&_technology
    16: sports
    17: travel_&_adventure
    18: youth_&_student_life
    """
    tokens = tokenizer(text, return_tensors='pt')
    output = model(**tokens)

    scores = output[0][0].detach().numpy()
    scores = expit(scores)
    predictions = (scores >= 0.5) * 1

    result_topics = []
    for cls_idx in np.where(predictions)[0]:
        result_topics.append(class_mapping[cls_idx])
    return result_topics

def main():
    df = read_toots_data()
    print("Number of toots = ", len(df))
    df['content_cleaned'] = df['content'].fillna('').apply(clean_html).apply(clean_string)
    
    # perform sentiment analysis
    df_with_sentiments = perform_sentiment_analysis(df)
    df_with_sentiments = df_with_sentiments[~df_with_sentiments['content_cleaned'].isna()]
    print("Number of toots after data cleansing = ", len(df_with_sentiments))
    df_topics = df_with_sentiments['content_cleaned'].apply(predict_topic)
    df_with_sentiments['topics'] = df_topics

    df_with_sentiments.to_parquet(f"{results_path}/df_mastodon_with_sentiments_and_topics.parquet")

if __name__ == '__main__':
    main()


        
