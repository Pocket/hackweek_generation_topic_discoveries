import streamlit as st
import pandas as pd
from utils import predict_topic

st.set_page_config(layout="wide")
st.title("[Draft] Community discovery App")
COLS_TOBE_SELECTED = ['user_id', 'note', 'content', 'username', 'display_name']
context = """:blue[_Help identify some User communities in Mastodon based on topics and keywords_]"""
st.markdown(context)


def get_public_toots_data():
    df = pd.read_parquet("data/results/df_for_discovery.parquet")
    return df.rename(columns={'note_cleaned': 'note',
                              'content_cleaned': 'content'})


df = get_public_toots_data()
topics_list = list(set([topic for topics in df['topics'].values.tolist() for topic in topics]))
topic_selected = st.selectbox("Choose a topic", topics_list + ['custom'])
if topic_selected == 'custom':
    keyword = st.text_input('Enter the topic keyword of your choice')
    predicted_topics = predict_topic(keyword)
    if len(predicted_topics):
        st.write(f"topic_selected =  **:green[{predicted_topics[0]}]**")
        topic_selected = predicted_topics[0]
    else:
        st.write("Try another keyword")
    

result = df[df['topics'].apply(lambda t: topic_selected in t)][COLS_TOBE_SELECTED]
if topic_selected == 'custom' and topic_selected is not None:
    st.write(f"Number of users found for {keyword} = {result['user_id'].nunique()}")
else:
    st.write(f"Number of users found for {topic_selected} = **:green[{result['user_id'].nunique()}]**")
st.table(result)