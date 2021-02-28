from locust import HttpUser, between, task
import random

class WebsiteUser(HttpUser):
    wait_time = between(5, 15)
    
    '''def on_start(self):
        self.client.post("/login", {
            "username": "test_user",
            "password": ""
        })'''
    
    @task
    def index(self):
        list_of_campaigns = [x for x in range(1,51)]
        campaign_id = random.choice(list_of_campaigns)
        self.client.get("/campaign/"+str(campaign_id))