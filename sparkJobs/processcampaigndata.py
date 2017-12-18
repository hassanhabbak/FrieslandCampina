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
        try:  # Attempt to cast the name to integer. Pass on fail
            dataset_folders.append(int(o.get('Prefix').split('/')[1]))
        except:
            pass

    print(dataset_folders)
    return dataset_folders


# Returns which dataset folder to process
def get_dataset_folder_to_process(dataset_folders, processed):
    print("Fetching processed history")
    last_processed_directory = None

    for row in processed.find().sort("_id", -1).limit(1):
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
    return "s3://frieslandcampinaassignment/csv/{}/{}_{}.csv".format(directory, file_name, directory)


# Is datasets a duplicate
def is_duplicate_datasets(processed, file_md5Sum):
    for file in file_md5Sum:
        if processed.find({file[0]+"_md5Sum": file[1]}).count() > 0:
            return True
        else:
            return False


# Log Process in MongoDB
def insert_process_history(db, folder_to_process, file_md5Sum, process_state, start_time):
    print("Inserting process history")
    post = {"order": folder_to_process,
            "state": process_state,
            "start_time": start_time,
            "end_time": datetime.datetime.now().isoformat()}
    for file in file_md5Sum:
        post[file[0]+"_md5Sum"] = file[1]
    db.processed.insert_one(post)


# Get list of keys in a specific bucket with a prefix
def get_matching_s3_Meta(s3, bucket, prefix=''):

    tuples = []
    kwargs = {'Bucket': bucket, 'Prefix': prefix}
    resp = s3.list_objects_v2(**kwargs)
    for obj in resp['Contents']:
        tuples.append((obj['Key'].split('/')[2].split('_')[0] ,obj['ETag']))

    return tuples


# Apply the transformations on the datasets to get performance per banner on campaign
def transform_banners_campaign_df(clicks_df, impressions_df, conversions_df):
    print("Transformation starts")

    # Grouped impressions by camp and bannerID
    impressions_grouped = impressions_df.groupBy(["campaign_id", "banner_id"]).count() \
        .withColumnRenamed("count", "impression_count")

    # Join conversions with clicks on click ID and with impressions on campaign_id and banner_id
    click_conversion_df = clicks_df.join(impressions_grouped, ["campaign_id", "banner_id"], "left_outer") \
        .join(conversions_df, "click_id", "left_outer") \
        .na.fill({'revenue': 0, 'conversion_id': 0})

    # Aggregate and calculate total revenue
    banners_campaigns_df = click_conversion_df \
        .withColumn("revenue_double", click_conversion_df.revenue.cast(DoubleType())) \
        .groupBy(click_conversion_df.campaign_id, click_conversion_df.banner_id) \
        .agg({'revenue_double': 'sum', 'click_id': 'count', 'impression_count': 'sum'}) \
        .withColumnRenamed("sum(revenue_double)", "revenue_total") \
        .withColumnRenamed("count(click_id)", "click_count") \
        .withColumnRenamed("sum(impression_count)", "impression_count")

    # Casting columns and renaming them
    banners_campaigns_df = banners_campaigns_df \
        .withColumn("campaign_id_new", banners_campaigns_df.campaign_id.cast(IntegerType())) \
        .drop("campaign_id").withColumnRenamed("campaign_id_new", "campaign_id") \
        .withColumn("banner_id_new", banners_campaigns_df.banner_id.cast(IntegerType())) \
        .drop("banner_id").withColumnRenamed("banner_id_new", "banner_id") \
        .withColumn("click_count_new", banners_campaigns_df.click_count.cast(IntegerType())) \
        .drop("click_count").withColumnRenamed("click_count_new", "click_count")

    return banners_campaigns_df


if __name__ == "__main__":

    # Job start time
    start_time = datetime.datetime.now().isoformat()

    # MongoDB connection
    mongo_client = pymongo.MongoClient('172.31.33.124', 27017)
    # mongo_client = pymongo.MongoClient('127.0.0.3', 3340) # Dev Connection
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
        .config("spark.mongodb.input.uri", "mongodb://bannersDB:SDFkl2423@172.31.33.124:27017/banners") \
        .config("spark.mongodb.output.uri", "mongodb://bannersDB:SDFkl2423@172.31.33.124:27017/banners") \
        .getOrCreate()

    # if the there are no folders to be processed
    if len(dataset_folders) == 0:
        print("No datasets found for processing")
        spark.stop()
        exit()

    folder_to_process = get_dataset_folder_to_process(dataset_folders, db.processed)

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

    # Get file CheckSum in the folder
    file_tuples = get_matching_s3_Meta(s3_client, BUCKET_NAME, 'csv/{}/'.format(folder_to_process))
    print("File Hash Keys: ", file_tuples)

    if is_duplicate_datasets(db.processed, file_tuples):
        print("Duplicate dataset already processed. Ignoring it!")
        insert_process_history(db, folder_to_process, file_tuples, "duplicate", start_time)
        spark.stop()
        exit()

    # Process and transform the data
    banners_campaigns_df = transform_banners_campaign_df(clicks_df, impressions_df, conversions_df)

    # Write the dataframe to MongoDB
    banners_campaigns_df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append") \
        .option("database", "banners") \
        .option("collection", "banner_performance_" + str(folder_to_process)) \
        .save()
    db["banner_performance_" + str(folder_to_process)].create_index([('field_i_want_to_index', pymongo.ASCENDING)])

    # Set record in dataset processed Collection
    insert_process_history(db, folder_to_process, file_tuples, "processed", start_time)

    mongo_client.close()
    spark.stop()
