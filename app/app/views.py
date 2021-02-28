from app import app
from flask import render_template
import pandas as pd
import json
import datetime
import math
import random
import pymongo


def load_image_from_database(req_time, campaign_id):
	connection = pymongo.MongoClient()
	db = connection['user_data']
	collection_banners = db['banners']
	doc = list(collection_banners.find({'campaign_id':str(campaign_id),'time_quarter':str(req_time)}))
	return doc[0]['list_of_banners']

@app.route("/campaign/<campaign_id>")
def index(campaign_id):
	current_time = datetime.datetime.now()
	file_pointer = math.ceil(current_time.minute / 15)
	if file_pointer == 0:
		file_pointer += 1
	#print (file_pointer, campaign_id)
	images_to_be_served = load_image_from_database(file_pointer, campaign_id)
	shuffled_images = tuple(random.sample(images_to_be_served, len(images_to_be_served)))
	shuffled_images = ['img/image_'+x+'.png' for x in shuffled_images]
	#print (shuffled_images)
	return render_template("index.html", results = shuffled_images)