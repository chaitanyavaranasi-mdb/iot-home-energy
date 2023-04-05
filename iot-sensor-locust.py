#!/usr/bin/env python

########################################################################
#
# This is an example Locust file that use Mimesis to help generate
# dynamic documents. Mimesis is more performant than Faker
# and is the recommended solution. After you build out your tasks,
# you need to test your file in mLocust to confirm how many
# users each worker can support, e.g. confirm that the worker's CPU
# doesn't exceed 90%. Once you figure out the user/worker ratio,
# you should be able to figure out how many total workers you'll need
# to satisfy your performance requirements.
#
# Note that these Mimesis locust files can be multi-use,
# saturating a database with data or demonstrating standard workloads.
# You should use the Host parameter to pass in task weights dynamically
# so you can switch between a bulk insert use case to running finds/aggs
# dynamically without having to redeploy a new Locust file.
#
########################################################################

# Allows us to make many pymongo requests in parallel to overcome the single threaded problem
from mimesis.schema import Field, Schema
from mimesis.locales import Locale
import string
from decimal import Decimal
import random
from datetime import datetime, timedelta
from pickle import TRUE
import time
from locust import User, events, task, constant, tag, between, runners
from bson.decimal128 import Decimal128
from bson import ObjectId
from bson.json_util import loads
from bson import json_util
import pymongo

import gevent
_ = gevent.monkey.patch_all()

########################################################################
# TODO Add any additional imports here.
# TODO But make sure to include in requirements.txt
########################################################################

# Global vars
# Make the task weights dynamic
weights = [1] * 20
# We can use this var to track the seq index of the worker in case we want to use it for generating unique seq keys in mimesis
worker_id = None


@events.init.add_listener
def on_locust_init(environment, **_kwargs):
    global worker_id
    if not isinstance(environment.runner, runners.MasterRunner):
        worker_id = environment.runner.worker_index


########################################################################
# mimesis schema for bulk creation
# The zoneKey is a great way to demonstrate zone sharding,
# e.g. all docs created by worker1 goes into shard1
# and all docs created by worker2 goes into shard2
# Note that increment doesn't maintain unique sequence numbers
# if you are running multiple mlocust users in parallel on the same worker
# Not every api func has been used. The full api can be found here. https://mimesis.name/en/master/api.html
# TODO Only use what you need. The more logic you have the slower your schema generation will be.
########################################################################
_ = Field(locale=Locale.EN)


class MetricsLocust(User):
    ########################################################################
    # Class variables.
    # The values are initialized with None
    # till they get set from the actual locust exeuction
    # when the host param is passed in.
    # DO NOT HARDCODE VARS! PASS THEM IN VIA HOST PARAM.
    # TODO Do you have more than 20 tasks? If so, change the array init below.
    ########################################################################
    client, coll, bulk_size = None, None, None

    global weights

    def __init__(self, parent):
        global schema, weights

        super().__init__(parent)

        try:
            vars = self.host.split("|")
            srv = vars[0]
            print("SRV:", srv)
            self.client = pymongo.MongoClient(srv)

            db = self.client[vars[1]]
            self.households = db['households']
            self.sensor_collection = db['sensorReadings']

            # docs to insert per batch insert
            self.bulk_size = int(vars[3])
            print("Batch size from Host:", self.bulk_size)
            # schema.create(iterations=self.bulk_size)

            # custom task frequency
            # otherwise everything is 1 (on)
            # Prepend weights to make sure we have enough array elements to prevent an error
            if (len(vars) > 4):
                listOfWeights = vars[4].split(",")
                lst_int = []
                for i in listOfWeights:
                    lst_int.append(int(i))
                weights = lst_int + weights
        except Exception as e:
            # If an exception is caught, Locust will show a task with the error msg in the UI for ease
            events.request.fire(request_type="Host Init Failure", name=str(
                e), response_time=0, response_length=0, exception=e)
            raise e

    ################################################################
    # Example helper function that is not a Locust task.
    # All Locust tasks require the @task annotation
    ################################################################
    def get_time(self):
        return time.time()

    ################################################################
    # Since the loader is designed to be single threaded with 1 user
    # There's no need to set a weight to the task.
    # Do not create additional tasks in conjunction with the loader
    # If you are testing running queries while the loader is running
    # deploy 2 clusters in mLocust with one running faker and the
    # other running query tasks
    # The reason why we don't want to do both loads and queries is
    # because of the simultaneous users and wait time between
    # requests. The bulk inserts can take longer than 1s possibly
    # which will cause the workers to fall behind.
    ################################################################
    # TODO Make sure to increment the array index for additional tasks
    @task(weights[0])
    def _insertOne(self):
        # global schema

        sensors = ["Water Heater", "AC Unit", "Refrigerator",
                   "Television", "Electric Oven", "Lights"]
        num_sensors = random.randint(1, len(sensors))
        household_sensors = random.choices(sensors, k=num_sensors)

        print(household_sensors)

        sensor_ids = random.sample(range(1000000, 1000001), num_sensors)

        longitude_range = (26.1, 49.0)
        latitude_range = (-124.6, -69.7)

        # Generate a random longitude and latitude within the range
        longitude = random.uniform(*longitude_range)
        latitude = random.uniform(*latitude_range)

        householdSchema = Schema(schema=lambda: {
            "sensors": sensor_ids,
            "geo": {
                "type": "Point",
                "coordinates": [latitude, longitude]
            }
        })

        sensor_array = []
        for index, sen in enumerate(household_sensors):
            sensor_doc = {
                "sensor_id": sensor_ids[index],
                "kwh": random.randint(100, 2000),
                "time": datetime.utcnow(),
                "type": sen
            }
            sensor_array.append(sensor_doc)

        # Note that you don't pass in self despite the signature above
        tic = self.get_time()
        name = "insertOne"

        time.sleep(.1)
        # print(schema*1)

        try:
            self.households.insert_many(householdSchema*1, ordered=False)
            self.sensor_collection.insert_many(sensor_array, ordered=False)

            events.request_success.fire(request_type="mlocust", name=name, response_time=(
                self.get_time()-tic)*1000, response_length=0)
        except Exception as e:
            events.request_failure.fire(request_type="mlocust", name=name, response_time=(
                self.get_timeme()-tic)*1000, response_length=0, exception=e)
            # Add a sleep so we don't overload the system with exceptions
            time.sleep(5)
