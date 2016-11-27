#!/usr/bin/env python
import re
import copy
import json
import boto3
import pandas
from functools import reduce
import io_handler
from textblob import TextBlob
from collections import Counter, defaultdict

s3 = io_handler.s3_client()
lambda_client = io_handler.lambda_client()
default_lambda_arn = 'arn:aws:lambda:eu-west-1:261000137328:function:pydata29-dev-process'

gender_pronouns = {
    'he': 'male',
    'him': 'male',
    'himself': 'male',
    'his': 'male',
    'she': 'female',
    'her': 'female',
    'herself': 'female',
    'hers': 'female',
}

re_cleaner = re.compile(r"[^a-z]")

def textblob_file_at_file_path(filepath):
    df = io_handler.load_jsonl_as_pandas(filepath)
    df.index = df.id
    df['blob'] = df.content.apply(TextBlob)
    df['tags'] = df.blob.apply(lambda b: b.tags)
    df['polarity'] = df.blob.apply(lambda b: b.sentiment.polarity)
    df['subjectivity'] = df.blob.apply(lambda b: b.sentiment.subjectivity)
    return df[['id', 'tags', 'polarity', 'subjectivity', 'published']]

def clean_token(tok):
    return re_cleaner.sub('', tok.lower())

def is_pronoun(tag):
    return tag in ['PRP', 'PRP$', 'WP', 'WP$']

def pronoun_gender(tok):
    return gender_pronouns.get(tok, 'neutral')

def positive_or_negative(polarity, subjectivity):
    is_positive = polarity > 0
    return 'positive' if is_positive else 'negative'

def count_tags_in_df(df):
    article_tag_count = defaultdict(Counter)
    for article_id, row in df.iterrows():
        tags = [(clean_token(tok), tag) for tok, tag in row.tags
                if is_pronoun(tag)]
        sentiment_agg = ['_{}'.format(
            positive_or_negative(row.polarity, row.subjectivity)
        )]
        gender_agg = ['_{}'.format(pronoun_gender(t)) for t, _ in tags]
        updates = tags + gender_agg + sentiment_agg
        article_tag_count[article_id].update(updates)
    counts_df = pandas.DataFrame.from_dict(article_tag_count, orient='index')
    return counts_df

def process_part(part_filepath):
    part = io_handler.describe_file_path(part_filepath)
    print("PROGRESS::Processing part {}...".format(part['name']))
    df = textblob_file_at_file_path(part_filepath)
    counts_df = count_tags_in_df(df)
    per_article_agg_cols = [c for c in counts_df.columns.values
                            if c.__class__ is ''.__class__ and
                               c.startswith('_')]
    merged = pandas.merge(df, counts_df[per_article_agg_cols],
                          right_index=True, left_index=True, how='outer')
    columns_to_drop = [c for c in merged.columns.values
                       if c not in ['polarity', 'subjectivity', 'published'] + per_article_agg_cols]
    per_article_df = merged.drop(columns_to_drop, 1)
    totals_series = counts_df.sum()

    root_parent = part['root_parent']
    article_file_name = '{}/articles/{}'.format(root_parent, part['name'])
    totals_file_name = '{}/totals/{}'.format(root_parent, part['name'])

    io_handler.write_pandas_to_csv(per_article_df, article_file_name,
                                   {'index_label' : 'id',
                                    'index' : True,
                                    'sep' : ';'})
    io_handler.write_pandas_to_csv(totals_series, totals_file_name,
                                   {'index_label' : 'agg',
                                    'header' : ['count'],
                                    'index' : True,
                                    'sep' : ';'})

    print("PROGRESS::Processed part {}!".format(part['name']))

def add_series(a, b):
    return a.add(b, fill_value=0)

def reduce_series_list(series_list):
    return reduce(add_series, series_list)

def concat_dataframe_list(df_list):
    return pandas.concat(df_list)

def process_filepath_in_lambda(filepath,
                               lambda_arn = default_lambda_arn):
    payload_dict = {
      "filepath": filepath,
    }
    payload = json.dumps(payload_dict)

    print('PROGRESS::Calling lambda with {} payload'.format(payload))
    lambda_client.invoke(
        FunctionName = lambda_arn,
        Payload      = payload
    )
    print('PROGRESS::Job with payload {}, done.'.format(payload))
    return 'done'

if __name__ == '__main__':
    from pprint import PrettyPrinter
    pp = PrettyPrinter()
    import random
    filepaths = io_handler.list_file_paths(data_root='s3://pydata-29/data')
    # filepaths = io_handler.list_file_paths()

    pp.pprint('...Process one part...')
    filepath = filepaths[random.choice(range(10000))]
    io_handler.describe_file_path(filepath)

    process_part(filepath)

    # if running from root in repl
    if False:
        import setup
        from processor import *

        filepaths = io_handler.list_file_paths()

        pp.pprint('...Textblobbing file at filepath...')
        filepath = filepaths[0]
        blobbed = textblob_file_at_file_path(filepath)
        d = blobbed.to_dict()
        pp.pprint({k: d[k] for k in list(d.keys())[:3]})
        pp.pprint(blobbed.columns.values)

        pp.pprint('...Counting stuff in file df...')
        counts_df = count_tags_in_df(blobbed)

        # I can now call process_part on a filepath and it will read it and write it... sort of.

        part_filepath = filepath
        counts_df = counts_df[per_article_counts]
        counts_df.columns.values
        counts_df.index = counts_df.index.rename('id')
        counts_df.index
        df = blobbed
        df.index

        merged
