import random
import pymongo
import time
import os
from datetime import datetime, timezone
from pymongo import MongoClient
from uuid import uuid4
from dotenv import load_dotenv

#Global Variable
sensor_array = []
load_dotenv()

def createSensors():
    sensors = []
    sensorType = ["Water Heater", "AC Unit", "Refrigerator", "Television", "Electric Oven", "Lights", "EV Charger", "Washer", "Dryer", "Dishwasher", "Microwave"]
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

def createHouseholds(): 
    longitude_range = (26.1, 49.0)
    latitude_range = (-124.6, -69.7)

    households = []

    for i in range(10000):
        household = {}
        household['householdId'] = i
        #household['coordinates'] = [random.uniform(latitude_range[0], latitude_range[1]),random.uniform(longitude_range[0], longitude_range[1])],
        household['coordinates'] = {"type": "Point",
                                    "coordinates": [random.uniform(latitude_range[0], latitude_range[1]),random.uniform(longitude_range[0], longitude_range[1])]},
        household['sensorIds'] = []
        for j in range(5):
            sensorId = str(uuid4())
            if sensorId not in sensor_array:
                sensor_array.append(sensorId)
                household['sensorIds'].append(sensorId)
            else: 
                j -= 1
        households.append(household)
    return households

#Connect to MongoDB
mongo_uri=f"{os.environ['CONNECTION_STRING']}"
client = pymongo.MongoClient(mongo_uri, 27017)
db = client['home-energy-management']
db.create_collection('sensors', timeseries={ 'timeField': 'timestamp' })

householdsCollection = db['households']
sensorCollection = db['sensors']
print('Connected to MongoDB')

#Insert Households into MongoDB
try: 
    households = createHouseholds()
    householdsCollection.insert_many(households)
    householdCount = householdsCollection.count_documents({})
    print('Households inserted. Number of Households: {}'.format(str(householdCount)))
    time.sleep(2)
except Exception as e:
    print('ERROR: Households not inserted')
    print(e)
    time.sleep(5)

#Insert Sensors into MongoDB
try: 
    sensors = createSensors()
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
    householdsCollection.create_index([('householdId', pymongo.ASCENDING)], unique=True)
    print('Household id index created')
    sensorCollection.create_index([('sensorId', pymongo.ASCENDING)])
    print('Sensor id index created')
    sensorCollection.create_index([('message', pymongo.ASCENDING)])
    print('Sensor message index created')
except Exception as e:
    print(e)
    time.sleep(5)
