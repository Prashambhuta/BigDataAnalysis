#!/usr/bin/python3


from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import os
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

URI = 'mongodb+srv://admin:commonpassword@cluster0.skomh.mongodb.net/myFirstDatabase' \
      '?retryWrites=true&w=majority '

client = MongoClient(URI)

db = client.test_database       # creating a test database

collection = db.test_collection # creating a test collection

post = {"name": ["Prasham", "Goda"],
        "skills": ["Pro", "Super"]
        }
posts = db.posts
post_id = posts.insert_one(post).inserted_id    # creating a document inside the
# collection
# post_id

# Checking for collections
print(db.list_collection_names())

