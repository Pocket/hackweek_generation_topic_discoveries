import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import MultiLabelBinarizer
import seaborn as sns
import requests
import json
from utils import make_clickable

response = requests.get("https://mastodon.social//api/v1/trends/tags")
statuses = json.loads(response.text) # this converts the json to a python list of dictionary

st.set_page_config(layout="wide")
st.title("Toots Sentiments, topics and related Pocket contents")


df = pd.read_parquet('data/results/df_mastodon_with_sentiments_and_topics.parquet')
df = df[['content_cleaned', 'label', 'score', 'topics']].rename(
    columns={'content_cleaned': 'toots',
             'label': 'sentiment',
             }
)
# df = df[~df['sentiment'].isna()]
st.write("Toots with sentiments and topics")
with st.expander("**:green[Toots and topic distribution]**", expanded=True):
    col1, col2, col3  = st.columns([5,4, 4], gap="small")
    with col1:

        st.write("#toots", len(df))
        st.write(df)
        mlb = MultiLabelBinarizer()

        df_final = df.join(pd.DataFrame(mlb.fit_transform(df.pop('topics')),
                                columns=mlb.classes_,
                                index=df.index))

        topics_list = mlb.classes_
    with col2:
        topic_dist = pd.DataFrame(df_final[topics_list].sum()).rename(columns={0: 'Counts'}).sort_values('Counts')
        plt.figure(figsize=(6, 6))
        fig, ax = plt.subplots()
        topic_dist['Counts'].plot(kind='barh')
        plt.title("Topic distribution of toots")
        st.pyplot(fig)
    with col3:
        approved_items = pd.read_csv("data/approved_with_topic_scores.csv")
        corpus_topic_dist = pd.DataFrame((approved_items[topics_list]>=0.5).sum()).rename(columns={0: 'Counts'}).sort_values('Counts')
        plt.figure(figsize=(6, 6))
        fig, ax = plt.subplots()
        corpus_topic_dist['Counts'].plot(kind='barh')
        plt.title("Approved Corpus Topic distribution")
        st.pyplot(fig)
    
topic_selected = st.selectbox("Choose the topic of your interest", topics_list)
with st.expander("**:green[Topic sentiments and trending hashtags]**", expanded=True):
    
    col1, col2, col3 = st.columns([3,2,3])
    with col1:

        toots_for_this_topic = df[df_final[topic_selected] == 1] \
                                .sort_values(
                                    ['sentiment','score'],
                                    ascending = [False, False]
                                    )
        st.write(toots_for_this_topic.reset_index(drop=True))
    with col2:
        st.write(f"Sentiments and counts for topic {topic_selected}")
        st.write(toots_for_this_topic['sentiment'].value_counts().reset_index() \
                    .rename(columns={'index': 'Sentiment', 'sentiment':'counts'}))

    with col3:
        trends = []
        for trend in json.loads(response.text):
            trends.append({'trend': trend['name'],
                           'url': trend['url']})
        trends_df = pd.DataFrame(trends)
        trends_df['url'] = trends_df['url'].apply(make_clickable)
        
        st.write(trends_df.to_html(escape=False, index=False), unsafe_allow_html=True)


approved_publishers = pd.read_csv("data/publishers_with_topic_scores.csv")
st.write("Number of approved items = ", len(approved_items))
with st.expander(f"**:green[Approved articles & publishers from pocket for the topic {topic_selected}]**", expanded=True):
    col1, col2 = st.columns([4,2], gap='small')
    with col1:
        approved_items["URL"] = approved_items["URL"].apply(make_clickable)
        disp = approved_items.sort_values(by=topic_selected, ascending=False).head(25)
        disp = disp[["TITLE", "TOP_DOMAIN_NAME", "URL"]].reset_index(drop=True)
        st.write(disp.to_html(escape=False, index=False), unsafe_allow_html=True)
    with col2:
        st.write(f"Top approved Publishers for {topic_selected}")
        disp = approved_publishers.sort_values(by=topic_selected, ascending=False).head(10)
        st.write(disp[["TOP_DOMAIN_NAME"]].reset_index(drop=True))
