import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import MultiLabelBinarizer
import seaborn as sns

st.title("Toots Sentiments and topics")

df = pd.read_parquet('../data/output/df_with_sentiments_and_topics.parquet')
df = df[['content_cleaned', 'label', 'score', 'topics']].rename(
    columns={'content_cleaned': 'toots',
             'label': 'sentiment',
             'score': 'sentiment_score',
             }
)
st.write("Toots with sentiments and topics")
st.write(df)
mlb = MultiLabelBinarizer()


df_final = df.join(pd.DataFrame(mlb.fit_transform(df.pop('topics')),
                          columns=mlb.classes_,
                          index=df.index))

topics_list = mlb.classes_
topic_selected = st.selectbox("Choose the topic of your interest", topics_list)
with st.expander("**:green[Topic sentiments]**", expanded=True):
    col1, col2 = st.columns([6,4], gap="small")
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
