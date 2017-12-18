# FrieslandCampina
FrieslandCampina Data Engineering assignment

We are creating a web​ application to serve banners for one of our many websites. The web​app should be smart enough to render banners based on their revenue ​performance. The project uses spark to process the performance of the banners per campaign. Then saves the results in MongoDB for Analaysis.

Later an AWS Lambda function is used to display the ads optimized for revenue, then clicks.

## Input

We get 4 datasets that contain banner information.

impressions.csv [banner_id, campaign_id] 
clicks.csv [click_id, banner_id, campaign_id] 
conversions.csv [conversion_id, click_id, revenue] 

## Banner Display
There are 4 scenarios:

    . banners with revenue >= 10 - Show the Top 10 banners based on revenue within that campaign 
    . banners with revenue inrange(5,9) Show the Top x banners based on revenue within that campaign X 
    . X in range(0,4) Show a mix of top revenue then top clicks
    . X = 0 Show random banners
    
All banners are randomized to avoid saturation.

## Tech Stack

### MongoDB
MongoDB is used mainly for storing the aggregated data from Spark Processing. It has a collection that keeps track of all processed files and their Hash to avoid duplication. It then adds the processed stats on a collection for quering from Lambda Function.

MongoDB is selected for database because of its balance of speed vs scalability. It allows for more complex queries, which could be very useful in gaining quick insights on the banner data. Due to the relatively small size of the data, I chose not to go with DynamoDB. Also because of the expense of DynamoDB, MongoDB seemed like a better approach.

### Amazon Lambda Function
Amazon Lambda function is great for Event based compute. In this case I used it to query MongoDB and render the ads based on the campaign requested. This removes the need for us to have a server running all the time. It also integerates very well with Amazon Cloudwatch and removes the hassle off of logging management. 

### Amazon S3
Amazon S3 is a reliable and extremely scalable storage system that is well integerated in Amazon Eco System. It is considerably fast to read from S3 from other amazon srevers. It is fault tolerant. Also provides a hashing of the files to avoid duplication.

### Amazon API GateWay
I use Amazon API Gateway as an endpoint to trigger Amazon Lambda function. This is great for the added security from Amazon Services, as well as the tight integeration with Amazon events. 

### Spark
Spark is a fantastic data processing Framework that allows us to process the data in any format and extract the insight needed from them. While spark might not be needed for the small datasets we have, it allows for scalability to any dataset size or structure in the future.

### Amazon EMR
Spark processes the data on EMR cluster. EMR comes with spark preinstalled and is extremely easy to scale. It provides monitoring capabilities and handles logging in a seamless way.

### Crontab
While my first choice would have been Amazon Data-pipeline, it was not available in the region I am hosting the solution. So Crontab seems like a simple solution to the issue since it can schedule the spark job on the cluster with very little problems.