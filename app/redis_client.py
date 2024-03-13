import redis
import json

class RedisClient:
    def __init__(self, host='localhost', port=6379, db=0):
        self._host = host
        self._port = port
        self._db = db
        self._client = redis.Redis(host=self._host, port=self._port, db=self._db)
    
    def close(self):
        self._client.close()

    def ping(self):
        return self._client.ping()
    
    def get(self, key):
        return self._client.get(key)
    
    def set(self, key, value):
        return self._client.set(key, value)
    
    def delete(self, key):
        return self._client.delete(key)
    
    def keys(self, pattern):
        return self._client.keys(pattern)
    
    def clearAll(self):
        for key in self._client.keys("*"):
            self._client.delete(key)

    # This method allows us to store a sensor variable data
    def add_sensor(self, key, value):
        # We will convert our sensor´s data into a JSON so we can easily store it under a single key
        self._client.set(key, json.dumps(value.dict()))
        # Once we have store it, we return the data on the DB to check everything was saved properly
        return json.loads(self._client.get(key))

    # This method given a key return
    def get_sensor(self, key):
        # Since we are saving JSONs on the data base, data will be stored as bytes. We will reconvert it
        # to its original type by performing json.loads
        return json.loads(self._client.get(key))
    
