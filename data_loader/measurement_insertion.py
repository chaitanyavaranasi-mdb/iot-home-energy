#!/usr/bin/env python
# Allows us to make many pymongo requests in parallel to overcome the single threaded problem
import pymongo
import time
import random
from datetime import datetime, timezone
from bson.json_util import dumps
from locust import User, events, task, tag, between
from bson import json_util
from bson.json_util import loads
from bson import ObjectId
import logging

from gevent import monkey
monkey.patch_all()

# Global vars
# Store the client conn globally so we don't create a conn pool for every user
# Track the srv globally so we know if we need to reinit the client
_CLIENT = None
_SRV = None

class MetricsLocust(User):
    client, coll = None, None

    def __init__(self, parent):
        global _CLIENT, _SRV
        super().__init__(parent)
        
        try:
            vars = []
            if self.host is not None: 
                vars = self.host.split("|")
            print("Host Param:",self.host)
            srv = vars[0]
            if _SRV != srv:
                self.client = pymongo.MongoClient(srv)
                _CLIENT = self.client
                _SRV = srv
            else:
                self.client = _CLIENT

            if self.client is not None:
                db = self.client[vars[1]]
                #self.coll = db[vars[2]]
                self.coll = db[vars[2]]
        except Exception as e:
            # If an exception is caught, Locust will show a task with the error msg in the UI for ease  # noqa: E501
            events.request.fire(request_type="Host Init Failure", name=str(e), response_time=0, response_length=0, exception=e)  # noqa: E501
            print(e)
            raise e
        
        try:
            cursor = None 
            if self.coll is not None: 
                cursor = self.coll.aggregate([
                    {
                        '$match': {
                            'message': 'Initiatize'
                        }
                    }, {
                        '$project': {
                            'sensorId': 1, 
                            '_id': 0,
                            'sensorType' : 1
                        }
                    }
                ])
            self.sensorArray = []
            if cursor is not None: 
                for record in cursor:
                    self.sensorArray.append(record)
        except Exception as e: 
            logging.info(e)
            logging.info("No sensors found")

    def get_time(self):
        return time.time()

    @task(100)
    def insertSensorData(self):
        tic = self.get_time()
        try:
            randomSensor = random.choice(self.sensorArray)
            sensorRecord = {}
            sensorRecord['sensorId'] = randomSensor['sensorId']
            sensorRecord['sensorType'] = randomSensor['sensorType']
            sensorRecord['sensorValue'] = random.randint(0, 100)
            sensorRecord['timestamp'] = datetime.now(timezone.utc)
            sensorRecord['heartbeat'] = 1
            sensorRecord['message'] = "Nominal Operation"
            if self.coll is not None: 
                self.coll.insert_one(sensorRecord)

            events.request.fire(request_type="Sensor Nominal Insert", name="insert_one_nominal", response_time=(self.get_time()-tic)*1000, response_length=0)  # noqa: E501
        except Exception as e:
            events.request.fire(request_type="Sensor Nominal Insert Failure", name=str(e), response_time=0, response_length=0, exception=e)  # noqa: E501
            logging.info(e)
            time.sleep(5)

    @task(1)
    def insertErrorSensorData(self):
        tic = self.get_time()
        try:
            randomSensor = random.choice(self.sensorArray)
            sensorRecord = {}
            sensorRecord['sensorId'] = randomSensor['sensorId']
            sensorRecord['sensorType'] = randomSensor['sensorType']
            sensorRecord['sensorValue'] = random.randint(0, 100)
            sensorRecord['timestamp'] = datetime.now(timezone.utc)
            sensorRecord['heartbeat'] = 1
            sensorRecord['message'] = "Error Operation"
            if self.coll is not None: 
                self.coll.insert_one(sensorRecord)
            
            events.request.fire(request_type="Sensor Error Insert", name="insert_one_error", response_time=(self.get_time()-tic)*1000, response_length=0)  # noqa: E501
        except Exception as e:
            events.request.fire(request_type="Sensor Error Insert failure", name=str(e), response_time=0, response_length=0, exception=e)  # noqa: E501
            logging.info(e)
            time.sleep(5)