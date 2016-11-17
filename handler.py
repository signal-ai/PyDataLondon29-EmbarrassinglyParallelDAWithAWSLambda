#!/usr/bin/env python

def process(event, context):
    return {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "event": event
    }
