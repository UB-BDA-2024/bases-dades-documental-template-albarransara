from pymongo import MongoClient,GEO2D
from bson.son import SON
from bson import json_util
import json
class MongoDBClient:
    def __init__(self, host="localhost", port=27017):
        self.host = host
        self.port = port
        self.client = MongoClient(host, port)
        self.database = None
        self.collection = None

    def close(self):
        self.client.close()
    
    def ping(self):
        return self.client.db_name.command('ping')
    
    def getDatabase(self, database):
        self.database = self.client[database]
        return self.database

    def getCollection(self, collection):
        self.collection = self.database[collection]
        return self.collection
    
    def clearDb(self,database):
        self.client.drop_database(database)

    def add_sensor(self, document):
        # Select database
        self.getDatabase("SensorsDB")
        # Select Sensor's collection
        col_sensors = self.getCollection("Sensors")
        # Insert sensor's data
        sensor = col_sensors.insert_one(document)
        # Create an index for location
        col_sensors.create_index([("location", "2dsphere")])
        return sensor

    def get_near_sensors(self, latitude, longitude,radius):
        # Select database
        self.getDatabase("SensorsDB")
        # Select collection
        col_sensors = self.getCollection("Sensors")
        # Query to find nearest sensors
        query = {"location": SON([("$near", {"$geometry": SON([("type", "Point"), ("coordinates", [latitude, longitude]), ("$maxDistance", radius)])})])}
        return json.loads(json_util.dumps(col_sensors.find(query)))

