#!/usr/bin/env python
from textblob_setup import setup
from pydata29 import processor, io_handler

# Download language model into AWS Lambda container.
# This is only done once per concurrent thread
# because the container is reused on subsequent calls.
setup()

def process(event, context):
    filepath = event['filepath']
    processor.process_part(filepath)
    return {
        "filepath": filepath,
    }
