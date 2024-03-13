from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional

from . import models, schemas

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, mongodb: Session, sensor: schemas.SensorCreate) -> models.Sensor:
    # Add data to Postgress
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    # Create a JSON document with the data
    data = {"id" : db_sensor.id,
            "name" : sensor.name,
            "type" : sensor.type,
            "mac_address" : sensor.mac_address,
            "manufacturer" : sensor.manufacturer,
            "model" : sensor.model,
            "serie_number" : sensor.serie_number,
            "firmware_version" : sensor.firmware_version,
            "location": { "type" : "Point", "coordinates" : [sensor.latitude, sensor.longitude]}}
    # Add data to MongoDB
    mongodb.add_sensor(data)
    return db_sensor

def record_data(redis: Session, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    # We will call an internal redis client method that allows us to store data under a key
    return redis.add_sensor(sensor_id, data)

def get_data(redis: Session, sensor_id: int) -> schemas.Sensor:
    # We will call an internal redis client method that allows us to get data under a key
    return redis.get_sensor(sensor_id)

def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor


def get_sensors_near(mongodb: Session, redisdb: Session, latitude, longitude, radius):
    # First get nearest sensor
    sensors = mongodb.get_near_sensors(latitude, longitude, radius)
    # If there is any sensor, add its variable data stored in Redis
    if sensors is not None:
        for i in range(len(sensors)):
            sensor_redis = get_data(redisdb,sensors[i]['id'])
            sensors[i]["temperature"] = sensor_redis["temperature"]
            sensors[i]["humidity"] = sensor_redis["humidity"]
            sensors[i]["battery_level"] = sensor_redis["battery_level"]
            sensors[i]["velocity"] = sensor_redis["velocity"]
            sensors[i]["last_seen"] = sensor_redis["last_seen"]
    return sensors
