import re
import string
from bs4 import BeautifulSoup
import spacy
import nltk
from nltk.stem import PorterStemmer
from nltk.stem.wordnet import WordNetLemmatizer
import spacy

nlp = spacy.load('en_core_web_sm')


def clean_html(html):
    """retrieve the tagged content"""
    soup = BeautifulSoup(html, "html.parser")
    for data in soup(['style', 'script', 'code', 'a']):
        # Remove tags
        data.decompose()
    return ' '.join(soup.stripped_strings)

def clean_string(text, stem="None"):
    """Clean the text"""

    final_string = ""

    # Make lower
    text = text.lower()

    # Remove line breaks
    text = re.sub(r'\n', '', text)

    # Remove puncuation
    translator = str.maketrans('', '', string.punctuation)
    text = text.translate(translator)

    # Remove stop words
    text = text.split()
    useless_words = nltk.corpus.stopwords.words("english")
    useless_words = useless_words + ['hi', 'im']

    text_filtered = [word for word in text if not word in useless_words]

    # Remove numbers
    text_filtered = [re.sub(r'\w*\d\w*', '', w) for w in text_filtered]

    # Stem or Lemmatize
    if stem == 'Stem':
        stemmer = PorterStemmer() 
        text_stemmed = [stemmer.stem(y) for y in text_filtered]
    elif stem == 'Lem':
        lem = WordNetLemmatizer()
        text_stemmed = [lem.lemmatize(y) for y in text_filtered]
    elif stem == 'Spacy':
        text_filtered = nlp(' '.join(text_filtered))
        text_stemmed = [y.lemma_ for y in text_filtered]
    else:
        text_stemmed = text_filtered

    final_string = ' '.join(text_stemmed)

    return final_string if len(final_string) <= 512 else final_string[:512]

def make_clickable(link):
    return f'<a target="_blank" href="{link}">{link}</a>'

from transformers import AutoModelForSequenceClassification, TFAutoModelForSequenceClassification
from transformers import AutoTokenizer
from scipy.special import expit
import numpy as np


MODEL = f"cardiffnlp/tweet-topic-21-multi"
tokenizer = AutoTokenizer.from_pretrained(MODEL)

model = AutoModelForSequenceClassification.from_pretrained(MODEL)
class_mapping = model.config.id2label


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