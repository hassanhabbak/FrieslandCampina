import unittest

from bson import ObjectId

from servebanner import servebanner
import mongomock


def test_valid_campaign_id():
    assert servebanner.valid_campaign_id(5) == True
    assert servebanner.valid_campaign_id("hi") == False


def test_get_latest_data_collection():
    collection = mongomock.MongoClient().db.collection
    objects = [dict(order=1), dict(order=2)]
    for obj in objects:
        obj['_id'] = collection.insert(obj)
    val = servebanner.get_latest_data_collection(collection)
    assert (val == 2)


"""
def test_set_banner_counts():
    collection = mongomock.MongoClient().db.collection
    objects = [{'impression_count':1, 'revenue_total':10, 'click_count':1, 'campaign_id':40}]
    for obj in objects:
        obj['_id'] = collection.insert(obj)

    camp_stats = servebanner.set_banner_counts(collection, 40)
    assert camp_stats.revenue_count == 20
    assert camp_stats.click_count == 2
    assert camp_stats.impressions_count == 2
"""


def test_generate_unique_list_of_banners():
    banner_ids = [6, 5, 4, 3, 2, 1]
    seen_banners = [1]
    result = servebanner.generate_unique_list_of_banners(banner_ids, seen_banners, 5)

    assert 1 not in result
    assert len(result) == 5
    assert {6, 5, 4, 3, 2}.issubset(result)


def test_get_list_of_banners_revenue_with_revenue():
    collection = mongomock.MongoClient().db.collection
    objects = [{'impression_count': 1, 'revenue_total': 10, 'click_count': 1, 'campaign_id': 40, 'banner_id': 1},
               {'impression_count': 1, 'revenue_total': 10, 'click_count': 1, 'campaign_id': 40, 'banner_id': 2},
               {'impression_count': 1, 'revenue_total': 10, 'click_count': 1, 'campaign_id': 40, 'banner_id': 3},
               {'impression_count': 1, 'revenue_total': 10, 'click_count': 1, 'campaign_id': 40, 'banner_id': 4},
               {'impression_count': 1, 'revenue_total': 10, 'click_count': 1, 'campaign_id': 40, 'banner_id': 5},
               {'impression_count': 1, 'revenue_total': 0, 'click_count': 1, 'campaign_id': 40, 'banner_id': 6}]
    for obj in objects:
        obj['_id'] = collection.insert(obj)
    val = servebanner.get_list_of_banners_revenue(collection, 40, [], 5)

    assert 6 not in val
    assert len(val) == 5


def test_get_list_of_banners_revenue_pick_at_random():
    collection = mongomock.MongoClient().db.collection
    objects = [{'impression_count': 1, 'revenue_total': 10, 'click_count': 1, 'campaign_id': 40, 'banner_id': 1},
               {'impression_count': 1, 'revenue_total': 10, 'click_count': 1, 'campaign_id': 30, 'banner_id': 2},
               {'impression_count': 1, 'revenue_total': 10, 'click_count': 1, 'campaign_id': 430, 'banner_id': 3},
               {'impression_count': 1, 'revenue_total': 10, 'click_count': 1, 'campaign_id': 420, 'banner_id': 4},
               {'impression_count': 1, 'revenue_total': 10, 'click_count': 1, 'campaign_id': 450, 'banner_id': 5},
               {'impression_count': 1, 'revenue_total': 0, 'click_count': 1, 'campaign_id': 460, 'banner_id': 6}]
    for obj in objects:
        obj['_id'] = collection.insert(obj)
    val = servebanner.get_list_of_banners_revenue(collection, 40, [], 5)

    assert 1 in val
    assert len(val) == 5


def test_get_seen_banners():
    val = servebanner.get_seen_banners("bla bla bla")
    assert val == []
    val = servebanner.get_seen_banners(
        "mycookiee=[373, 124, 114, 472, 322]; seen_banners_cookie=[446, 121, 334, 500, 182]")
    assert val == [446, 121, 334, 500, 182]
