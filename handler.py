#!/usr/bin/env python
from textblob_setup import setup
from textblob import TextBlob

setup()

def process(event, context):
    text = "Go Serverless v1.0! Your function executed successfully!"
    print("Blobbing the text {}".format(text))
    blob = TextBlob(text)
    print("Finished blobbing!")
    return {
        "text": text,
        "tags": blob.tags,
        "event": event
    }
