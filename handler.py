#!/usr/bin/env python
from textblob_setup import setup
from textblob import TextBlob

setup()

def process(event, context):
    text = event['text']
    print("Blobbing the text:\n----{}".format(text))
    blob = TextBlob(text)
    print("Finished blobbing!")
    return {
        "text": text,
        "tags": blob.tags,
        "sentiment": blob.sentiment
    }

if False:

    from handler import *

    process(None, None)
