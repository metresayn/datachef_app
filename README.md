# datachef_app
Home assignment

## Data Pipeline
1) Created a python script to load the data from csvs to pyspark dataframe.
2) The python script calculates the scores for each campaign and stores in json.
3) The python script loads json data to mongo db instance (already installed mongo db service).
4) A flask app is deployed to serve banners. This flask app serves the requests in the following domain: http://127.0.0.1:5000/campaign/campaign_id. Here, campaign_id has the value between (1, 50).
5) The views.py file in the flask app creates a collection and stores it in a local dataframe. The flask app gives responses on the basis of the dataframe.



## Steps to run the application
1) Install required packages.
2) Set the environment variables for pyspark_python and pyspark_driver_python
3) Setup and install mongodb, start the mongodb service.
4) For newly created mongo service, create database -> user_data.
5) Create a collection, collection in user_data -> user_data.banners.
6) Create an index on this collection -> db.banners.createIndex({"campaign_id": 1,"time_quarter": 1}, {unique: true})
7) Now run the flask app from the directory: app/ -> flask run


## AWS service app:
1) Created an ec2 instance to run the flask app.
2) The app is running on ip: http://3.139.233.25/campaign/32 (campaign_id should be between (1-50) to render banners).


## Test files
1) Testcases are written specific to testing the python script in Data/tests/.
2) The locustfile.py can be used to run the load testing of the app. Loading testing results are present in folder Load_test_data/.

## Functional Requirements check
- [x] To avoid saturation for visitors, we believe that the top banners being served, should not follow a fixed order based on its performance; but they should appear in a random sequence.
- [ ] You should also avoid having a banner served twice in a row for a unique visitor.
    - This is not done. But there is a certain randomness for every visitor. So a unique visitor will not rendered with the same order of banners.
- [x] And finally, the 4 sets of CSV's represent the 4 quarters of an hour. So when I visit the website during 00m-15m, I want to see banners being served based on the statistics of the first dataset, and if I visit your site during 16m-30m, I want to see the banners being served based on the second dataset, so on so forth.


## Non-Functional Requirements Check
- [x] Your application should serve at least 5000 requests per minute. The script and results of the stress test should be provided.
- [x] Loading data from CSV files, make sure you have de-duplication logic in place. If I add a duplicate csv file or part of content is duplicate of previous files, then gracefully reject the duplicate but log it somewhere.
   - De-duplication logic is in place at csv level. So if someone adds duplicate entries in a csv it will be filtered and logged in a folder duplicates/. But there      is no logic in place for duplicate folders with similar content.
- [x]  Code should be tested (be it in unittest or another library) both for green path scenario and alternative scenarios like duplicate csv, etc.
   - Unit Testing is not done on python script, not on flask app.
- [x] Deploy to AWS (deployment automation is not needed but will be a big plus if you do!).
- [x] Code must be pushed to a public Github or CodeCommit repo (temporarily). 
