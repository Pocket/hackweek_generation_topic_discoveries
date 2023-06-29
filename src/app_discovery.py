import streamlit as st
import pandas as pd
from utils import predict_topic

st.set_page_config(layout="wide")
st.title("Community discovery App")
COLS_TOBE_SELECTED = ['user_id','note','username','display_name']
context = """:blue[_Help identify some User communities in Mastodon based on topics and keywords_]"""
st.markdown(context)
topics_list = ['arts_&_culture', 'business_&_entrepreneurs', 'celebrity_&_pop_culture',
       'diaries_&_daily_life', 'family', 'fashion_&_style', 'film_tv_&_video',
       'fitness_&_health', 'food_&_dining', 'gaming', 'learning_&_educational',
       'music', 'news_&_social_concern', 'other_hobbies', 'relationships',
       'science_&_technology', 'sports', 'travel_&_adventure',
       'youth_&_student_life']


def get_public_toots_data():
    df = pd.read_parquet("data/results/df_for_discovery.parquet")
    return df.rename(columns={'note_cleaned': 'note',
                              'content_cleaned': 'content'})

def get_results(df, topic_selected):
    result = df[df[topic_selected] > 0][COLS_TOBE_SELECTED + [topic_selected]]
    result = result.sort_values(topic_selected, ascending=False).reset_index(drop=True).rename(columns={topic_selected: 'toot_counts'}) 
    return result

df = get_public_toots_data()
topic_selected = st.selectbox("Choose a topic", topics_list + ['custom'])
if topic_selected == 'custom':
    keyword = st.text_input('Enter the topic keyword of your choice')
    predicted_topics = predict_topic(keyword)
    if len(predicted_topics):
        st.write(f"topic_selected =  **:green[{predicted_topics[0]}]**")
        topic_selected = predicted_topics[0]
    else:
        st.write("**:red[Please try another keyword]**")
    
if topic_selected != 'custom':
    results = get_results(df, topic_selected)
    st.table(results)
