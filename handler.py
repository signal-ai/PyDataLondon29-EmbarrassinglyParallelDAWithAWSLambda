#!/usr/bin/env python
from textblob_setup import setup
from pydata29 import processor, io_handler

setup()

def process(event, context):
    filepath = event['filepath']
    processor.process_part(filepath)
    return {
        "filepath": filepath,
    }

if False:

    from handler import *

    # process(None, None)
