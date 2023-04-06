The intent of this repository is to build out a demo for the MongoDB Product, Time Series Collections. 

The demo will simulate sensor readings from a number of households in the United States

Instructions: 
1. Copy .env.sample to .env and modify the connection string 
~~~
cd time-series-demo
cp .env.sample .env
~~~
2. Run the scripts in data-loader
~~~
cd data_loader
pip3 install -r requirements.txt
python3 household_generation.py
~~~
3. The Sensor generation will utilize locust, a load testing library. You can use MongoDB's managed mLocust or you can spin up an independent locust instance. The code example below is for the local locust instance. You can reference the locust configuration options [here](https://docs.locust.io/en/stable/configuration.html). 
~~~
locust -f data_loader/measurement_insertion.py --headless -u 1 -r 1 --run-time 1m --host '<CONNECTION_STRING>|home-energy-management|sensors'
~~~
