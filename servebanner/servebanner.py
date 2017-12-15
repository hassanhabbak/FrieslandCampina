import collections
import random

import boto3
import json
import pymongo

print('Loading function')

# Handles bad request message
def handle_bad_request(message, code):
    content = """<html>
                    <head>
                        <title>400 Bad Request</title>
                    </head>
                    <body>
                        <h1>Bad Request</h1>
                        <p>Your browser sent a request that this server could not understand.<p>
                        <p>""" + message + """<p>
                    </body>
                </html>"""
    return {
        "statusCode": code,
        "body": content,
        "headers": {
            'Content-Type': 'text/html'
        }
    }

# Validate the Campaign ID
def valid_campaign_id(camp_id):
    try:
        val = int(camp_id)
        return True
    except ValueError:
        return False

# Gets the latest collection from DB
def get_latest_data_collection(db):
    last_processed_directory = None
    for row in db.processed.find().sort("_id", -1).limit(1):
        last_processed_directory = row['order']

    return last_processed_directory

# Retreives the banner counts based on ones with impressions, revenue and clicks
def set_banner_counts(db, collection_name, serving_camp_id):
    collection_cursor = db[collection_name].aggregate([
        {
            "$match": {"campaign_id": serving_camp_id}
        },
        {"$group": {
            "_id": None,
            "impression_count": {
                "$sum": 1
            },
            "sum_revenue": {
                "$sum": {
                    "$cond": [{"$gt": ["$revenue_total", 0]}, 1, 0]
                }
            },
            "sum_clicks": {
                "$sum": {
                    "$cond": [{"$gt": ["$click_count", 0]}, 1, 0]
                }
            }
        }}
    ])

    # Count of more than zero banner stats
    impressions_count, revenue_count, click_count = 0, 0, 0
    for row in collection_cursor:
        impressions_count = row['impression_count']
        revenue_count = row['sum_revenue']
        click_count = row['sum_clicks']

    stats = collections.namedtuple('stats', ['impressions_count', 'revenue_count', 'click_count'])
    stats.impressions_count = impressions_count
    stats.revenue_count = revenue_count
    stats.click_count = click_count

    return stats

# generate a unique list of banners that are not seen previously if possible
def generate_unique_list_of_banners(banner_ids, seen_banners, num_of_banners):
    result_banner_ids = []

    # select 10 unseen if possible
    for banner_id in banner_ids:
        if banner_id not in seen_banners:
            result_banner_ids.append(banner_id)
        if len(result_banner_ids) == num_of_banners:
            break

    # Incase we ran out of banners to show because of the exclusion
    if len(result_banner_ids) < num_of_banners:
        for banner_id in banner_ids:
            if banner_id not in result_banner_ids:
                result_banner_ids.append(banner_id)
            if len(result_banner_ids) == num_of_banners:
                break

    # Randomize the list
    random.shuffle(result_banner_ids)

    return result_banner_ids

# Get list of banners based on limit and seen banners
def get_list_of_banners_revenue(db, collection_name, serving_camp_id, seen_banners, limit):
    banner_ids = []

    collection_cursor = db[collection_name] \
        .find({'campaign_id': serving_camp_id}) \
        .sort([('revenue_total', -1),
               ('click_count', -1)])\
        .limit(limit + len(seen_banners))

    for row in collection_cursor:
        banner_ids.append(row['banner_id'])

    # In case we could not find enough banners, pick at random to make-up the limit
    if len(banner_ids) < limit:
        # Get all banner IDs not already in selection
        all_banner_ids = db[collection_name] \
            .find({'banner_id':{'$nin':banner_ids}}) \
            .distinct('banner_id')

        random.shuffle(all_banner_ids)
        for id in all_banner_ids:
            banner_ids.append(id)
            if len(banner_ids) >= limit:
                break

    return generate_unique_list_of_banners(banner_ids, seen_banners, limit)

# Get the list of banners to display
def get_list_of_banners(db, serving_camp_id, last_processed_collection, seen_banners):
    banner_ids = []
    collection_name = "banner_performance_"+str(last_processed_collection)

    # Count of more than zero banner stats
    camp_stats = set_banner_counts(db, collection_name, serving_camp_id)
    print("Banners with impressions = ", camp_stats.impressions_count)
    print("Banners with revenue_count = ", camp_stats.revenue_count)
    print("Banners with click count = ", camp_stats.click_count)

    if camp_stats.revenue_count >= 10:
        # More than 10 banners with revenue, serve from that
        banner_ids = get_list_of_banners_revenue(db, collection_name, serving_camp_id, seen_banners, 10)
        print("Case revenue count banners > 10")
    elif camp_stats.revenue_count >= 5:
        # if more than 5 with revenue, serve them
        banner_ids = get_list_of_banners_revenue(db, collection_name, serving_camp_id, [], camp_stats.revenue_count)
        print("Case revenue count banners > 10")
    elif camp_stats.revenue_count >= 0:
        # if more than 0 with revenue
        banner_ids = get_list_of_banners_revenue(db, collection_name, serving_camp_id, [], 5)
        print("Case revenue count < 5")

    print("Banner IDs list ", banner_ids)
    return banner_ids

# Builds the HTMl with the images for display
def build_html(banner_ids):
    image_html = "<img src='https://s3.eu-central-1.amazonaws.com/frieslandcampinaassignment/images/image_{}.png'>"
    images = ''
    for id in banner_ids:
        images = images + image_html.format(id)

    return "<html><body>" + images + "</body></html>"

# Handles the event from Lambda function
def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    # Verify that it is a get request
    if event["httpMethod"] != "GET":
        print("Invalid Request! " + event["httpMethod"])
        return handle_bad_request("The request method is unsupported!", 400)

    # Verify that campaign parameter was sent
    if "campaign_id" not in event["pathParameters"] or \
        ("campaign_id" not in event["pathParameters"] and not valid_campaign_id(event["pathParameters"]["campaign_id"])):
        print("Invalid Campaign ID! " + event["httpMethod"])
        return handle_bad_request("The request method is unsupported!", 400)

    # MongoDB connection
    # This is creating a connection everytime
    # A better approach is to keep a connection pool
    # outside of Lambda AWS
    mongo_client = pymongo.MongoClient('172.31.33.124', 27017)
    # mongo_client = pymongo.MongoClient('127.0.0.3', 3340) # Dev Connection
    db = mongo_client["banners"]
    db.authenticate("bannersDB", "SDFkl2423")

    # Gets the latest collection number
    last_processed_collection = get_latest_data_collection(db)

    # Handle no data found!
    if last_processed_collection is None:
        print("No collections found!")
        return handle_bad_request("No Data found!", 500)

    # Campaign ID to serve
    serving_camp_id = int(event["pathParameters"]["campaign_id"])

    banner_ids = get_list_of_banners(db, serving_camp_id, last_processed_collection, [])
    content = build_html(banner_ids)

    mongo_client.close()

    return {
        "statusCode": 200,
        "body": content,
        "headers": {
            'Content-Type': 'text/html',
            'Cookie': 'testcookie1=12345; domain=localhost:8000; expires=Thu, 19 Apr 2018 20:41:27 GMT;'
        }
    }




"<html><body>" \
"<img src='<img src='https://s3.eu-central-1.amazonaws.com/frieslandcampinaassignment/images/image_475.png'>" \
"<img src='https://s3.eu-central-1.amazonaws.com/frieslandcampinaassignment/images/image_325.png'>" \
"<img src='https://s3.eu-central-1.amazonaws.com/frieslandcampinaassignment/images/image_330.png'>" \
"<img src='https://s3.eu-central-1.amazonaws.com/frieslandcampinaassignment/images/image_290.png'>" \
"<img src='https://s3.eu-central-1.amazonaws.com/frieslandcampinaassignment/images/image_435.png'>'>" \
"</body></html>"