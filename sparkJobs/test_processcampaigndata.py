import logging
import unittest
import mongomock
import pytest
import findspark
from pyspark import SparkConf, SparkContext

from sparkJobs import processcampaigndata

def test_get_folder_to_process():
    collection = mongomock.MongoClient().db.collection
    objects = [dict(order=1), dict(order=2)]
    for obj in objects:
        obj['_id'] = collection.insert(obj)
    val = processcampaigndata.get_dataset_folder_to_process([1,2,3,4],collection)
    assert(val == 3)

    collection.insert(dict(order=4))

    val = processcampaigndata.get_dataset_folder_to_process([1,2,3,4],collection)
    assert(val is None)


def test_duplicate_datasets():
    collection = mongomock.MongoClient().db.collection
    objects = [dict(min_click_ID=10, max_click_ID=20), dict(min_click_ID=20, max_click_ID=30)]
    for obj in objects:
        obj['_id'] = collection.insert(obj)
    val = processcampaigndata.is_duplicate_datasets(collection, 10, 20)
    assert(val == True)

    val = processcampaigndata.is_duplicate_datasets(collection, 10, 30)
    assert(val is False)

    val = processcampaigndata.is_duplicate_datasets(collection, 30, 40)
    assert (val is False)