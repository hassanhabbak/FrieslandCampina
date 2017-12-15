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