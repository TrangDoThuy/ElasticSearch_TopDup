# -*- coding: utf-8 -*-
"""
Created on Sat Jan  9 23:17:48 2021

@author: trang
my code is based on this article "https://www.elastic.co/blog/how-to-find-and-remove-duplicate-documents-in-elasticsearch"
"""

#!/usr/local/bin/python3
import hashlib
import csv
import json
import time
from elasticsearch import Elasticsearch, helpers
ES_HOST = {"host": "localhost", "port": 9200}
es = Elasticsearch(hosts=[ES_HOST])
dict_of_duplicate_docs = {}
dict_of_simlilar_docs = []
score_threshold = 5

def csv_reader(file_obj, delimiter=','):
    reader = csv.DictReader(file_obj)
    i = 1
    results = []
    for row in reader:
        #print(row)
        es.index(index='document', id=i,
                         body=json.dumps(row))
        i = i + 1

        results.append(row)
        #print(row)


def compute_time(method):
  # Function computing time decorator
  def timed(*args, **kwargs):
    start_time = time.time()
    result = method(*args, **kwargs)
    print("--- %s seconds ---" % (time.time() - start_time))
    return result
  return timed

# Process documents returned by the current search/scroll
def populate_dict_of_duplicate_docs(hit):

    combined_key = ""
    #for mykey in keys_to_include_in_hash:
    try:
        combined_key += str(hit['_source']['column2'])
    except:
        combined_key += str(hit['_source']['content'])
    _id = hit["_id"]

    hashval = hashlib.md5(combined_key.encode('utf-8')).digest()

    # If the hashval is new, then we will create a new key
    # in the dict_of_duplicate_docs, which will be
    # assigned a value of an empty array.
    # We then immediately push the _id onto the array.
    # If hashval already exists, then
    # we will just push the new _id onto the existing array
    dict_of_duplicate_docs.setdefault(hashval, []).append(_id)


# Loop over all documents in the index, and populate the
# dict_of_duplicate_docs data structure.
@compute_time
def scroll_over_all_docs():
    for hit in helpers.scan(es, index='document'):
        #print(hit)
        populate_dict_of_duplicate_docs(hit)


def loop_over_hashes_and_print_duplicates():
    # Search through the hash of doc values to see if any
    # duplicate hashes have been found
    for hashval, array_of_ids in dict_of_duplicate_docs.items():
      if len(array_of_ids) > 1:
        print("********** Duplicate docs hash=%s **********" % hashval)
        # Get the documents that have mapped to the current hasval
        matching_docs = es.mget(index="document", doc_type="doc", body={"ids": array_of_ids})
        for doc in matching_docs['docs']:
            # we print duplicate docs.
            print("doc=%s\n" % doc)
            print(doc['_id'])
            response = es.get(index='document', id=doc['_id'])
            
            print(response)
            print('\n')



def main():
    scroll_over_all_docs()
    print()
    #loop_over_hashes_and_print_duplicates()


main()