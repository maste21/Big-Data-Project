import sys
import datetime
import cv2
from kafka import KafkaProducer
from json import dumps
import numpy as np
import base64
import random

topic = "hello"

def publish_camera():
    # Start up producer
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: x.encode('utf-8'))


    camera = None
    camera_index_found = -1
    for i in range(5):  # Try indices 0 to 4
        temp_camera = cv2.VideoCapture(i)
        if temp_camera.isOpened():
            camera = temp_camera
            camera_index_found = i
            print(f"Successfully opened camera at index: {i}")
            break
        else:
            print(f"No camera found at index: {i}")
            temp_camera.release() # Release if not opened to avoid resource leak

    if camera is None:
        print("Error: Could not open any camera. Please check camera connections and permissions.")
        return 

    message = None
    d = 0
    while(True):
        success, frame = camera.read()

        if not success:
            print("Failed to grab frame. Exiting loop.")
            break # Exit the loop if frame reading fails

        frame = cv2.resize(frame, (300, 200)) 
        rows = 480 
        cols = 640 
        cam_id = random.randint(1,30)
        ret, buffer = cv2.imencode('.jpg', frame)
        data = base64.b64encode(buffer)
        message = {
            "camera_id" : cam_id,
            "date" : str(datetime.date.today()),
            "time":datetime.datetime.now().strftime("%X"),
            "rows" : rows,
            "cols" : cols,
            "data" : str(data)
        }
        json_data = dumps(message)
        print(json_data)
        producer.send(topic, value=json_data)
        producer.flush()
        print('Message published successfully.')

    camera.release() # Release camera resource when the loop breaks


if __name__ == '__main__':
    print("publishing feed!")
    publish_camera()