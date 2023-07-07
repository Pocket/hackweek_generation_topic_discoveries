# This module helps prepare the data for discovery
# In a nutshell this uses the data from replied toots in mastodon.
# Then we take a sample of that and run topic model and enrich that

import tarfile
import pandas as pd
import glob
from utils import clean_html, clean_string, predict_topic
from sklearn.preprocessing import MultiLabelBinarizer

def extract_from_tar_file():
    file = tarfile.open('./data/replied_toots_2023_05_27.tar.gz')
    file.extractall('./data/')
    file.close()

def read_data_from_extracted_dir():
    datasets = glob.glob('./data2/replied_toots_2023_05_27/status_mastodon*.parquet')
    df = pd.concat([pd.read_parquet(data) for data in datasets], axis=0)
    df['user_id'] = df['account'].apply(lambda details: details['id'])
    df['note'] = df['account'].apply(lambda details: details['note'])
    df['display_name'] = df['account'].apply(lambda details: details['display_name'])
    df['username'] = df['account'].apply(lambda details: details['username'])
    df = df[(df['content'].apply(len) < 256) & (df['language'] == 'en')]
    df = df.drop_duplicates(subset=['id'])
    # df = df.sample(3000)
    print(f"After sampling we got {len(df)} rows")
    df = df[(df['content'] != '' )].reset_index(drop=True)
    return df

def clean_text(df):
    df['content_cleaned'] = df['content'].fillna('').apply(clean_html).apply(clean_string)
    df['note_cleaned'] = df['note'].fillna('').apply(clean_html).apply(clean_string)
    df = df[(~df['content_cleaned'].isna()) & (~df['note_cleaned'].isna())]
    print("Number of toots after data cleansing = ", len(df))
    return df.copy().reset_index(drop=True)
    
def obtain_topics_for_content(df_cleaned):
    df_cleaned['topics'] = df_cleaned['content_cleaned'].apply(predict_topic)
    return df_cleaned

def get_summary_by_user_topics(df_for_discovery):
    mlb = MultiLabelBinarizer()
    df_summarized_by_user = df_for_discovery[['user_id', 'note_cleaned', 'username', 'display_name']].join(pd.DataFrame(mlb.fit_transform(df_for_discovery['topics']), columns=mlb.classes_, index=df_for_discovery.index))
    df_for_discovery = df_summarized_by_user.groupby(['user_id', 'note_cleaned', 'username', 'display_name']).sum().reset_index()
    return df_for_discovery

if __name__ == '__main__':
    # extract_from_tar_file()
    df = read_data_from_extracted_dir()
    df_users_and_notes = clean_text(df)
    print(f"Starting Prediction for {len(df_users_and_notes)}.  This will take a while")
    df_for_discovery = obtain_topics_for_content(df_users_and_notes)
    print(f"Number of rows in df_for_discovery = {len(df_for_discovery)}")
    df_for_discovery = get_summary_by_user_topics(df_for_discovery)
    print("df_for_discovery \n")
    print(df_for_discovery.head())
    print(df_for_discovery.columns)
    df_for_discovery.to_parquet("data/results/df_for_discovery.parquet")