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
st.title("Toots Sentiments and topics")


# df = pd.read_parquet('data/output/df_with_sentiments_and_topics.parquet')
df = pd.read_parquet('data/results/df_mastodon_with_sentiments_and_topics.parquet')
df = df[['content_cleaned', 'label', 'score', 'topics']].rename(
    columns={'content_cleaned': 'toots',
             'label': 'sentiment',
             'score': 'sentiment_score',
             }
)
df = df[~df['sentiment'].isna()]
st.write("Toots with sentiments and topics")
st.write(df)
mlb = MultiLabelBinarizer()


df_final = df.join(pd.DataFrame(mlb.fit_transform(df.pop('topics')),
                          columns=mlb.classes_,
                          index=df.index))


topics_list = mlb.classes_
topic_selected = st.selectbox("Choose the topic of your interest", topics_list)
with st.expander("**:green[Topic sentiments]**", expanded=True):
    col1, col2, col3 = st.columns([4,2,3], gap='small')
    with col1:

        toots_for_this_topic = df[df_final[topic_selected] == 1] \
                                .sort_values(
                                    ['sentiment','sentiment_score'],
                                    ascending = [False, False]
                                    )
        st.write(toots_for_this_topic)
    with col2:
        plt.figure(figsize=(5,5))
        fig, ax = plt.subplots()
        toots_for_this_topic['sentiment'].value_counts().plot(kind='barh')
        plt.title(f"Topic {topic_selected} by sentiments")
        st.pyplot(fig)
    with col3:
        trends = []
        for trend in json.loads(response.text):
            trends.append({'trend': trend['name'],
                           'url': trend['url']})
        trends_df = pd.DataFrame(trends)
        trends_df['url'] = trends_df['url'].apply(make_clickable)
        st.write(trends_df.to_html(escape=False, index=False), unsafe_allow_html=True)


approved_items = pd.read_csv("data/approved_with_topic_scores.csv")
approved_publishers = pd.read_csv("data/publishers_with_topic_scores.csv")
approved_items["URL"] = approved_items["URL"].apply(make_clickable)
st.write(f"Approved Items for {topic_selected}:")
disp = approved_items.sort_values(by=topic_selected, ascending=False).head(30)
disp = disp[["TITLE", "TOP_DOMAIN_NAME", "URL"]].reset_index()
st.write(disp.to_html(escape=False, index=False), unsafe_allow_html=True)

st.write(f"Top approved Publishers for {topic_selected}")
disp = approved_publishers.sort_values(by=topic_selected, ascending=False).head(10)
st.write(disp[["TOP_DOMAIN_NAME"]].reset_index())

