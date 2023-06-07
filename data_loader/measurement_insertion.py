#!/usr/bin/env python
# Allows us to make pymongo requests in parallel to overcome the single threaded problem
import pymongo
import time
import random
from datetime import datetime, timezone
from locust import User, events, task
import logging
from pymongo import MongoClient
from uuid import uuid4
from faker import Faker
from geopy.geocoders import Nominatim

from gevent import monkey
monkey.patch_all()

# Global vars
# Store the client conn globally so we don't create a conn pool for every user
# Track the srv globally so we know if we need to reinit the client
_CLIENT = None
_SRV = None
sensor_array = []
fake = Faker()
geolocator = Nominatim(user_agent="iot_geoapi")
numHouseholds = 10000
numSensorsPerHousehold = 5

class MetricsLocust(User):
    client, coll = None, None

    def __init__(self, parent):
        global _CLIENT, _SRV
        super().__init__(parent)
        
        #Setup MongoDB Connection
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
            raise e
        
        self.client = pymongo.MongoClient(srv, 27017)
        db = self.client['home-energy-management']
        list_of_collections = db.list_collection_names()
        
        try: 
            if 'sensors' not in list_of_collections:
                db.create_collection('sensors', timeseries={ 'timeField': 'timestamp' })
                sensorCollection = db['sensors']
                householdsCollection = db['households']
            else: 
                sensorCollection = db['sensors']
                householdsCollection = db['households']
        except Exception as e: 
            events.request.fire(request_type="Sensors collection creation error", name=str(e), response_time=0, response_length=0, exception=e)  # noqa: E501
            raise e

        #Insert Households into MongoDB if collection if empty
        try: 
            households = self.createHouseholds()
            householdsCollection.insert_many(households)
            householdCount = householdsCollection.count_documents({})
            print('Households inserted. Number of Households: {}'.format(str(householdCount)))  # noqa: E501
            time.sleep(2)
        except Exception as e:
            print('ERROR: Households not inserted')
            print(e)
            time.sleep(5)

        #Insert Sensors into MongoDB if collection if empty
        try: 
            sensors = self.createSensors()
            sensorCollection.insert_many(sensors)
            sensorCount = sensorCollection.count_documents({})
            print('Sensors inserted. Number of Sensors: {}'.format(str(sensorCount)))
            time.sleep(2)
        except Exception as e:
            print('ERROR: Sensors not inserted')
            print(e)
            time.sleep(5)

         #Create Indexes in MongoDB 
        try:
            householdsCollection.create_index([('householdId', pymongo.ASCENDING)], unique=True)  # noqa: E501
            print('Household id index created')
            sensorCollection.create_index([('sensorId', pymongo.ASCENDING)])
            print('Sensor id index created')
            sensorCollection.create_index([('message', pymongo.ASCENDING)])
            print('Sensor message index created')
        except Exception as e:
            print(e)
            time.sleep(5)

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

    def get_time(self):
        return time.time()

    def createSensors(self):
        sensors = []
        sensorType = ["Water Heater", "AC Unit", "Refrigerator", "Television", "Electric Oven", "Lights", "EV Charger", "Washer", "Dryer", "Dishwasher", "Microwave"]  # noqa: E501
        for i in range(len(sensor_array)):
            sensor = {}
            sensor['sensorId'] = sensor_array[i]
            sensor['sensorType'] = random.choice(sensorType)
            sensor['sensorValue'] = random.randint(0, 100)
            sensor['timestamp'] = datetime.now(timezone.utc)
            sensor['heartbeat'] = 1
            sensor['message'] = "Initiatize"
            sensors.append(sensor)
        return sensors

    def createHouseholds(self): 
        households = []

        for i in range(numHouseholds):
            householdCoordinates = fake.local_latlng(country_code="US")
            
            household = {}
            household['householdId'] = i
            household['region'] = householdCoordinates[3]
            household['city'] = householdCoordinates[2]
            household['coordinates'] = {"type": "Point",
                                        "coordinates": [householdCoordinates[0],householdCoordinates[1]]}  # noqa: E501
            household['sensorIds'] = []

            for j in range(numSensorsPerHousehold):
                sensorId = str(uuid4())
                if sensorId not in sensor_array:
                    sensor_array.append(sensorId)
                    household['sensorIds'].append(sensorId)
                else: 
                    j -= 1
            households.append(household)
        return households
    
    def generateKwhValue(self): 
        return True