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
    objects = [dict(clicks_md5Sum="10", impressions_md5Sum="20", conversions_md5Sum="30"),
               dict(clicks_md5Sum="40", impressions_md5Sum="50", conversions_md5Sum="60")]
    for obj in objects:
        obj['_id'] = collection.insert(obj)
    val = processcampaigndata.is_duplicate_datasets(collection, [("clicks", "10")])
    assert(val == True)

    val = processcampaigndata.is_duplicate_datasets(collection, [("conversions", "10")])
    assert(val is False)

    val = processcampaigndata.is_duplicate_datasets(collection,[("clicks", "200"), ("conversions", "10")])
    assert (val is False)