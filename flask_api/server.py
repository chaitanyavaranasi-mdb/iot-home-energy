from flask import Flask
import pymongo
from bson import json_util
import json
import os


from flask_cors import CORS
app = Flask(__name__,             
            static_url_path='', 
            static_folder='templates/static',)
CORS(app)

mongo_uri=f"{os.environ['CONNECTION_STRING']}"
client = pymongo.MongoClient(mongo_uri, 27017)
db = client['home-energy-management']
householdsCollection = db['households']
sensorCollection = db['sensors']
print('Connected to MongoDB')

@app.route('/healthCheck')
def printValues():
    count_sensors = sensorCollection.count_documents({})
    count_households = householdsCollection.count_documents({})
    return f'Number of sensors: {count_sensors} and number of households: {count_households}'  # noqa: E501

@app.route('/getLatestValues', methods=['GET'])
def getLatestValues(sensorId, numofValues):
    valueArray = []
    agg_pipeline = [{
        '$match': {'sensorId': sensorId},
        '$sort': {'timestamp': -1},
        '$limit': numofValues
    }]
    valueArray = list(sensorCollection.aggregate(agg_pipeline))
    return valueArray

@app.route('/getHouseholds', methods=['GET'])
def getHouseholds():
    return json.loads(json_util.dumps(householdsCollection.find()))

@app.route('/getIndividualSensor', methods=['GET'])
def getSensorIndividual(sensorId):
    return json.loads(json_util.dumps(sensorCollection.find({'sensorId': sensorId})))

@app.route('/getSensorPerHousehold', methods=['GET'])
def getSensorPerHousehold(householdId):
    return json.loads(json_util.dumps(sensorCollection.find({'householdId': householdId})))  # noqa: E501

# @app.route('/densifySensorData', methods=['GET'])

# @app.route('/windowSensorData', methods=['GET'])

# @app.route('/modifySensorValue', methods=['POST'])

# @app.route('/getSensorData', methods=['GET'])
# def getWindowData(sensorId, startTime, endTime):
#     valueArray = []
#     agg_pipeline = [{
#         '$match': {'sensorId': sensorId},
#         '$sort': {'timestamp': -1},
#         '$limit': numofValues
#     }]
#     valueArray = list(sensorCollection.aggregate(agg_pipeline))
#     return valueArray