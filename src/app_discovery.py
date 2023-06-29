import streamlit as st
import pandas as pd
from utils import predict_topic

st.set_page_config(layout="wide")
st.title("Community discovery App")
COLS_TOBE_SELECTED = ['user_id', 'content', 'display_name', 'degree', 'betweenness_centrality', 'eigenvector_centrality']
context = """:blue[_Help identify User communities based on topic and central influence]"""
st.markdown(context)

def get_public_toots_topic_data():
    df = pd.read_parquet("data/results/df_for_discovery_35k_full.parquet")
    return df

def get_centality_data():
    df = pd.read_parquet("data/results/centrality_15K.parquet")
    return df


toots_df = get_public_toots_topic_data()
centrality_df = get_centality_data()
merged_df = toots_df.merge(centrality_df, left_on='user_id', right_on='ID', how='left')

topics_list = list(set([topic for topics in merged_df['topics'].values.tolist() for topic in topics]))
topic_selected = st.selectbox("Choose a topic", topics_list + ['custom'])
if topic_selected == 'custom':
    keyword = st.text_input('Enter the topic keyword of your choice')
    predicted_topics = predict_topic(keyword)
    if len(predicted_topics):
        st.write(f"topic_selected =  **:green[{predicted_topics[0]}]**")
        topic_selected = predicted_topics[0]
    else:
        st.write("Try another keyword")
    


result = merged_df[merged_df['topics'].apply(lambda t: topic_selected in t)][COLS_TOBE_SELECTED].sort_values('betweenness_centrality', ascending=False)
result['degree'] = result['degree'].round(0)
result['betweenness_centrality'] = (result['betweenness_centrality'] * 100).round(2)
result['eigenvector_centrality'] = (result['eigenvector_centrality'] * 100).round(2)

if topic_selected == 'custom' and topic_selected is not None:
    st.write(f"Number of users found for {keyword} = {result['user_id'].nunique()}")
else:
    st.write(f"Number of users found for {topic_selected} = **:green[{result['user_id'].nunique()}]**")


result = result.drop_duplicates(['user_id'], keep='first')

column_weights = [1.0, 4]
cols = st.columns(len(COLS_TOBE_SELECTED))

for col, field in zip(cols, COLS_TOBE_SELECTED):
    col.write("**" + field + "**")

for i, row in result.iloc[0:100].iterrows():
    cols = st.columns(len(COLS_TOBE_SELECTED))

    for c, col_name in enumerate(COLS_TOBE_SELECTED):
        if col_name == 'content':
            cols[c].markdown(row[col_name], unsafe_allow_html=True)
        elif col_name == "user_id":
            url = "https://mastodon.social/api/v1/accounts/{}".format(row[col_name])
            display_name = row['display_name']
            cols[c].markdown(f"<a style='text-decoration:none;' href={url} target='_blank' rel='noopener noreferrer'>{display_name}</a>",
                             unsafe_allow_html=True)

        else:
            cols[c].markdown(row[col_name], unsafe_allow_html=False)
