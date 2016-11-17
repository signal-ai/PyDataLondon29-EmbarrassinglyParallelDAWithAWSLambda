#!/usr/bin/env python
import os
os.environ['NLTK_DATA'] = '/tmp'
from textblob import download_corpora

def setup():
    print("Downloading corpora...")
    download_corpora.download_lite()
    print("Finished downloading!")
