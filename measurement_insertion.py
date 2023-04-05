import pymongo
import time
import random
from bson.json_util import dumps
from locust import HttpUser, events, task, constant, tag, between, runners

class MetricsLocust(HttpUser):

    wait_time = between(1,30)
    client, coll, bulk_size = None, None, None
    global weights, schema

    def __init__(self, parent):
        super().__init__(parent)
        
        try:
            vars = self.host.split("|")
            srv = vars[0]
            print("SRV:",srv)
            self.client = pymongo.MongoClient(srv)

            db = self.client[vars[1]]
            self.sensors = db['sensors']

        except Exception as e:
            # If an exception is caught, Locust will show a task with the error msg in the UI for ease
            # events.fire(request_type="Host Init Failure", name=str(e), response_time=0, response_length=0, exception=e)
            print(e)
            raise e
        
        try: 
            cursor = self.sensors.aggregate([
                {
                    '$match': {
                        'message': 'Initiatize'
                    }
                }, {
                    '$project': {
                        'sensorIds': 1, 
                        '_id': 0,
                        'sensorType' : 1
                    }
                }
            ])
            self.sensorArray = []
            for record in cursor:
                self.sensorArray.append(record)
        except: 
            print("No sensors found")

    @task
    def insertSensorData(self):
        try:
            randomSensor = random.choice(self.sensorArray)
            sensorRecord = {}
            sensorRecord['sensorId'] = randomSensor['sensorIds']
            sensorRecord['sensorType'] = randomSensor['sensorType']
            sensorRecord['sensorValue'] = random.randint(0, 100)
            sensorRecord['timestamp'] = time.time()
            sensorRecord['heartbeat'] = 1
            sensorRecord['message'] = "Nominal Operation"
            self.sensors.insert_one(sensorRecord)
        except Exception as e:
            print(e)
            time.sleep(5)

    @task
    def insertErrorSensorData(self):
        try:
            randomSensor = random.choice(self.sensorArray)
            sensorRecord = {}
            sensorRecord['sensorId'] = randomSensor['sensorIds']
            sensorRecord['sensorType'] = randomSensor['sensorType']
            sensorRecord['sensorValue'] = random.randint(0, 100)
            sensorRecord['timestamp'] = time.time()
            sensorRecord['heartbeat'] = 1
            sensorRecord['message'] = "Error Operation"
            self.sensors.insert_one(sensorRecord)
        except Exception as e:
            print(e)
            time.sleep(5)