# This module helps prepare the data for discovery
# In a nutshell this uses the data from replied toots in mastodon.
# Then we take a sample of that and run topic model and enrich that

import tarfile
import pandas as pd
import glob
from utils import clean_html, clean_string, predict_topic
  
COLS_TO_BE_SELECTED = ['user_id', 'note_cleaned', 'content_cleaned', 'username', 'display_name']

def extract_from_tar_file():
    file = tarfile.open('/Users/cgopal/Downloads/replied_toots_2023_05_27.tar.gz')
    # print(file.getnames())
    file.extractall('./jag_data')
    file.close()

def read_data_from_extracted_dir():
    datasets = glob.glob('./jag_data/replied_toots_2023_05_27/*.parquet')
    df = pd.concat([pd.read_parquet(data) for data in datasets], axis=0)
    df['user_id'] = df['account'].apply(lambda details: details['id'])
    df['note'] = df['account'].apply(lambda details: details['note'])
    df['display_name'] = df['account'].apply(lambda details: details['display_name'])
    df['username'] = df['account'].apply(lambda details: details['username'])
    df = df[(df['content'].apply(len) < 256) & (df['language'] == 'en')]
    df = df.sample(3000)
    print(f"After sampling we got {len(df)} rows")
    df = df[(df['content'] != '' )].reset_index(drop=True)
    return df

def clean_text(df):
    df['content_cleaned'] = df['content'].fillna('').apply(clean_html).apply(clean_string)
    df['note_cleaned'] = df['note'].fillna('').apply(clean_html).apply(clean_string)
    df = df[(~df['content_cleaned'].isna()) & (~df['note_cleaned'].isna())]
    print("Number of toots after data cleansing = ", len(df))
    return df[COLS_TO_BE_SELECTED].copy().reset_index(drop=True)
    
def obtain_topics_for_content(df_cleaned):
    df_cleaned['topics'] = df_cleaned['content_cleaned'].apply(predict_topic)
    return df_cleaned


if __name__ == '__main__':
    extract_from_tar_file()
    df = read_data_from_extracted_dir()
    df_users_and_notes = clean_text(df)
    df_for_discovery = obtain_topics_for_content(df_users_and_notes)
    print(f"Number of rows in df_for_discovery = {len(df_for_discovery)}")
    print("df_for_discovery \n")
    print(df_for_discovery.head())
    df_for_discovery.to_parquet("data/results/df_for_discovery.parquet")