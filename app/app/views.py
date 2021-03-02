from app import app
from flask import render_template
import pandas as pd
import json
import datetime
import math
import random
import pymongo

connection = pymongo.MongoClient()
db = connection['user_data']
collection_banners = db['banners']
df = pd.DataFrame(list(collection_banners.find()))

def load_image_from_database(df, req_time, campaign_id):
	#print (df.head(5))
	doc = df.loc[(df["campaign_id"] == str(campaign_id)) & (df["time_quarter"] == str(req_time))]['list_of_banners'].tolist()
	return doc
	
@app.route("/")
def home_func():
    return render_template("head.html")

@app.route("/campaign/<campaign_id>")
def index(campaign_id):
	current_time = datetime.datetime.now()
	file_pointer = math.ceil(current_time.minute / 15)
	if file_pointer == 0:
		file_pointer += 1
	#print (file_pointer, campaign_id)
	images_to_be_served = load_image_from_database(df, file_pointer, campaign_id)[0]
	#print (images_to_be_served)
	shuffled_images = random.sample(images_to_be_served, len(images_to_be_served))
	#print (shuffled_images)
	shuffled_images = ['img/image_'+x+'.png' for x in shuffled_images]
	#print (shuffled_images)
	return render_template("index.html", results = shuffled_images)