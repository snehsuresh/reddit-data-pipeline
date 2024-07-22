import sys
import os
import numpy as np
import pandas as pd
import praw
from praw import Reddit
import re

from utils.constants import POST_FIELDS, ARTISTS_DATA_PATH
artist_data_path = os.path.join(ARTISTS_DATA_PATH, "merged_file.csv")

# Load the artist data
try:
    artist_df = pd.read_csv(artist_data_path)
except FileNotFoundError:
    print(f"File {artist_data_path} not found.")
    sys.exit(1)

def connect_reddit(client_id, client_secret, user_agent) -> Reddit:
    try:
        reddit = praw.Reddit(client_id=client_id,
                             client_secret=client_secret,
                             user_agent=user_agent)
        print("Connection to reddit successful")
        return reddit
    except Exception as e:
        print(e)
        sys.exit(1)


def extract_posts(reddit_instance: Reddit, subreddit: str, time_filter: str, limit=None):
    subreddit = reddit_instance.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)

    post_lists = []

    for post in posts:
        post_dict = vars(post)
        post = {key: post_dict[key] for key in POST_FIELDS}
        post_lists.append(post)

    return post_lists

def clean_text(text):
    return re.sub(r'\W+', '', text).lower()

def find_longest_match(title, names):
    # Clean and normalize title
    cleaned_title = clean_text(title)
    best_match = None
    best_match_length = 0

    for name in names:
        cleaned_name = clean_text(name)
        if cleaned_name in cleaned_title:
            if len(cleaned_name) > best_match_length:
                best_match = name
                best_match_length = len(cleaned_name)
    
    return best_match

def transform_data(post_df: pd.DataFrame):

    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    post_df['over_18'] = np.where((post_df['over_18'] == True), True, False)
    post_df['author'] = post_df['author'].astype(str)
    edited_mode = post_df['edited'].mode()
    post_df['edited'] = np.where(post_df['edited'].isin([True, False]),
                                 post_df['edited'], edited_mode).astype(bool)
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['score'] = post_df['score'].astype(int)
    post_df['title'] = post_df['title'].astype(str)
    
    artist_names = artist_df['name'].dropna().tolist()

    for index, row in post_df.iterrows():
        title = row['title']
        best_artist = find_longest_match(title, artist_names)
        
        if best_artist:
            # Find the genre corresponding to the best match
            genre = artist_df[artist_df['name'] == best_artist]['genre'].values
            genre = genre[0] if len(genre) > 0 else ''
            
            post_df.at[index, 'artist'] = best_artist
            post_df.at[index, 'genre'] = genre

    # Fill any missing 'genre' values with "Mix"
    post_df['genre'] = post_df['genre'].replace("", "Mix")
    

    return post_df


def load_data_to_csv(data: pd.DataFrame, path: str):
    data.to_csv(path, index=False)