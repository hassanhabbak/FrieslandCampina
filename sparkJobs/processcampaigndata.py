import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType
import boto3
import pymongo

BUCKET_NAME = 'frieslandcampinaassignment'
DATASET_PREFIX = 'csv/'

# Grabs the subfolder names in the CSV folder on S3 Bucket
def get_dataset_folder_names(s3_client):
    dataset_folders = []
    result = s3_client.list_objects(Bucket=BUCKET_NAME, Prefix=DATASET_PREFIX, Delimiter='/')
    for o in result.get('CommonPrefixes'):
        try: # Attempt to cast the name to integer. Pass on fail
            dataset_folders.append(int(o.get('Prefix').split('/')[1]))
        except:
            pass

    print(dataset_folders)
    return dataset_folders

# Returns which dataset folder to process
def get_dataset_folder_to_process(dataset_folders, db):
    print("Fetching processed history")
    last_processed_directory = None

    for row in db.processed.find().sort("_id", 1).limit(1):
        last_processed_directory = row['order']

    # No previous records, start from the top
    if last_processed_directory is None:
        print("No History Found, Time begins now!")
        return dataset_folders[0]

    # If last folder is also the last processed
    if last_processed_directory == dataset_folders[-1]:
        print("No New Data to Process")
        return None

    # Find the next folder in sequence to process
    folder_number = dataset_folders[-1]
    for i in reversed(dataset_folders):
        if i <= last_processed_directory:
            break
        folder_number = i

    return folder_number

# Formats the dataset filename
def get_file_path(directory, file_name):
    return "csv/{}/{}_{}.csv".format(directory, file_name, directory)

# Is datasets a duplicate
def is_duplicate_datasets(db, min_click_id, max_click_id):
    if db.mycollection.find({'min_click_ID': min_click_id, 'max_click_ID': max_click_id}).count() > 0:
        return True
    else:
        return False

# Log Process in MongoDB
def insert_process_history(db, folder_to_process, click_min_id, click_max_id, process_state, start_time):
    print("Inserting process history")
    post = {"order": folder_to_process,
            "min_click_ID": click_min_id,
            "max_click_ID": click_max_id,
            "state": process_state,
            "start_time": start_time,
            "end_time": datetime.datetime.now().isoformat()}
    db.processed.insert_one(post)

# Apply the transformations on the datasets to get performance per banner on campaign
def transform_banners_campaign_df(clicks_df, impressions_df, conversions_df):
    print ("Transformation starts")

    # Grouped impressions by camp and bannerID
    impressions_grouped = impressions_df.groupBy(["campaign_id", "banner_id"]).count() \
        .withColumnRenamed("count", "impression_count")

    impressions_grouped.show()

    # Join conversions with clicks on click ID and with impressions on campaign_id and banner_id
    click_conversion_df = clicks_df.join(impressions_grouped, ["campaign_id", "banner_id"], "left_outer") \
        .join(conversions_df, "click_id", "left_outer") \
        .na.fill({'revenue': 0, 'conversion_id': 0})

    # Aggregate and calculate total revenue
    banners_campaigns_df = click_conversion_df \
        .withColumn("revenue_double", click_conversion_df.revenue.cast(DoubleType())) \
        .groupBy(click_conversion_df.campaign_id, click_conversion_df.banner_id) \
        .agg({'revenue_double': 'sum', 'click_id': 'count'}) \
        .withColumnRenamed("sum(revenue_double)", "revenue_total") \
        .withColumnRenamed("count(click_id)", "click_count")

    # Casting columns and renaming them
    banners_campaigns_df = banners_campaigns_df \
        .withColumn("campaign_id_new", banners_campaigns_df.campaign_id.cast(IntegerType())) \
        .drop("campaign_id").withColumnRenamed("campaign_id_new", "campaign_id") \
        .withColumn("banner_id_new", banners_campaigns_df.banner_id.cast(IntegerType())) \
        .drop("banner_id").withColumnRenamed("banner_id_new", "banner_id") \
        .withColumn("click_count", banners_campaigns_df.click_count.cast(IntegerType())) \
        .drop("click_count").withColumnRenamed("click_count_new", "click_count")

    return banners_campaigns_df

# Get max click IDs
def get_max_click_ids(clicks_df):
    click_max_id = clicks_df.withColumn("click_id_int", clicks_df.click_id.cast(IntegerType())) \
        .agg({'click_id_int': 'max'}).collect()[0][0]
    print("Max Click ID is ", click_max_id)
    return click_max_id

# Get min click IDs
def get_min_click_ids(clicks_df):
    click_min_id = clicks_df.withColumn("click_id_int", clicks_df.click_id.cast(IntegerType())) \
        .agg({'click_id_int': 'min'}).collect()[0][0]
    print("Min Click ID is ", click_min_id)
    return click_min_id

if __name__ == "__main__":

    # Job start time
    start_time = datetime.datetime.now().isoformat()

    # MongoDB connection
    # Dev conn
    # mongo_client = pymongo.MongoClient('127.0.0.3', 3340)
    mongo_client = pymongo.MongoClient('172.31.33.124', 27017)
    db = mongo_client["banners"]
    db.authenticate("bannersDB", "SDFkl2423")

    # Getting S3 bucket
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(BUCKET_NAME)

    s3_client = boto3.client('s3')
    dataset_folders = get_dataset_folder_names(s3_client)

    # Spark session configs
    spark = SparkSession \
        .builder \
        .appName("processCampaignData") \
        .config("spark.mongodb.input.uri", "mongodb://bannersDB:SDFkl2423@127.0.0.3:3340/banners") \
        .config("spark.mongodb.output.uri", "mongodb://bannersDB:SDFkl2423@127.0.0.3:3340/banners") \
        .getOrCreate()

    # if the there are no folders to be processed
    if len(dataset_folders) == 0:
        print("No datasets found for processing")
        spark.stop()
        exit()

    folder_to_process = get_dataset_folder_to_process(dataset_folders, db)

    # If folder to process could not be found
    if folder_to_process is None:
        print("Failed to find a folder to process")
        spark.stop()
        exit()

    # Loading CSV files
    clicks_df = spark.read.csv(get_file_path(folder_to_process, "clicks"), header=True)
    conversions_df = spark.read.csv(get_file_path(folder_to_process, "conversions"), header=True)
    impressions_df = spark.read.csv(get_file_path(folder_to_process, "impressions"), header=True)

    clicks_df.printSchema()
    conversions_df.printSchema()
    impressions_df.printSchema()

    # Get minimum and maximum click IDs for duplicate detection
    click_max_id = get_max_click_ids(clicks_df)
    click_min_id = get_min_click_ids(clicks_df)

    if is_duplicate_datasets(db, click_min_id, click_max_id):
        print("Duplicate dataset already processed. Ignoring it!")
        insert_process_history(db, folder_to_process, click_min_id, click_max_id, "duplicate", start_time)
        spark.stop()
        exit()

    # Process and transform the data
    banners_campaigns_df = transform_banners_campaign_df(clicks_df, impressions_df, conversions_df)

    # Write the dataframe to MongoDB
    banners_campaigns_df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append")\
        .option("database","banners")\
        .option("collection", "banner_performance_" + str(folder_to_process))\
        .save()

    # Set record in dataset processed Collection
    insert_process_history(db, folder_to_process, click_min_id, click_max_id, "processed", start_time)

    mongo_client.close()
    spark.stop()
