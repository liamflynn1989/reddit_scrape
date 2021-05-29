import praw
import sys
from praw.models import MoreComments
import pandas as pd
import os
import time
import datetime as dt #only if you want to analyze the date created feature
from dotenv import load_dotenv
load_dotenv()

current_time = int(time.time())
topic = sys.argv[1]

# Authenticate
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
client_id = REDDIT_CLIENT_ID
client_secret = REDDIT_CLIENT_SECRET
user_agent = 'test0'
reddit = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)


def parse_submission(sub_id):

    submission = reddit.submission(sub_id)
    data = [(c.body,c.author,c.score,0,c.created_utc) for c in submission.comments if not isinstance(c, MoreComments) and len(c.body)>25]
    df = pd.DataFrame(data=data, columns = ['comment','author','score','type','utc'])

    if len(submission.selftext) > 25:

        df.loc[len(df)] = [submission.selftext,submission.author,submission.score, 1, submission.created_utc]

    if len(submission.title) > 25:
        df.loc[len(df)] = [submission.title,submission.author,submission.score, 2, submission.created_utc]

    df['sub_id'] = sub_id

    return df

# Get hot posts on topic subreddit
hot_posts = reddit.subreddit(topic).hot(limit=50)
id_list = [post.id for post in hot_posts]

# Get comments on all posts
comments_df = pd.DataFrame()

for sub_id in id_list:
    comments_df = comments_df.append(parse_submission(sub_id))
    print(len(comments_df),end='\r')

comments_df['author'] = comments_df['author'].astype(str)


comments_df.reset_index(inplace=True,drop=True)
# Only take comments from the last 24 hours
comments_df = comments_df.loc[comments_df['utc']>current_time-86400]

comments_df.to_json(f'comments/{topic}_{current_time}.json')

duration = int(time.time()) - current_time

print(f'Finished {topic} in {duration} seconds. {len(comments_df)} comments collected.')






