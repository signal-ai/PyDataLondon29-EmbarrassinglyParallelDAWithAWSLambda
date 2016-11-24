#!/usr/bin/env python
import re
import copy
import boto3
import pandas
from functools import reduce
import io_handler
from textblob import TextBlob
from collections import Counter, defaultdict

s3 = io_handler.s3_client()

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
    return df[['id', 'tags', 'polarity', 'subjectivity']]

def clean_token(tok):
    return re_cleaner.sub('', tok.lower())

def is_pronoun(tag):
    return tag in ['PRP', 'PRP$', 'WP', 'WP$']

def pronoun_gender(tok):
    return gender_pronouns.get(tok, 'neutral')

def positive_or_negative(polarity, subjectivity):
    is_positive = ((subjectivity >= 0.5 and polarity >= 0) or
                   (polarity >= 0.7))
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
                       if c not in ['polarity', 'subjectivity'] + per_article_agg_cols]
    per_article_df = merged.drop(columns_to_drop, 1)
    totals_series = counts_df.sum()

    article_file_name = 'articles/{}'.format(part['name'])
    totals_file_name = 'totals/{}'.format(part['name'])

    if part.get('bucket'):
        d = per_article_df
        csv = d.to_csv(None,
                       index_label='id',
                       index=True,
                       sep=';')
        a_response = s3.put_object(Body   = csv,
                                   Bucket = part['bucket'],
                                   Key    = article_file_name)
        d = totals_series
        csv = d.to_csv(None,
                       index=True,
                       header=['count'],
                       index_label='agg',
                       sep=';')
        t_response = s3.put_object(Body   = csv,
                                   Bucket = part['bucket'],
                                   Key    = totals_file_name)
    else:
        root = '/'.join(part['root'].split('/')[:-1])
        per_article_df.to_csv('{}/{}'.format(root, article_file_name),
                              index_label='id',
                              index=True,
                              sep=';')
        totals_series.to_csv('{}/{}'.format(root, totals_file_name),
                             index=True,
                             header=['count'],
                             index_label='agg',
                             sep=';')

    print("PROGRESS::Processed part {}!".format(part['name']))

def add_series(a, b):
    return a.add(b, fill_value=0)

def reduce_series_list(series_list):
    return reduce(add_series, series_list)

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
