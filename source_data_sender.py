import pika, sys, os, json
import pandas as pd

df = pd.read_csv("test.csv")

#Rabbit MQ setup
connection = pika.BlockingConnection(
pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='bts_in3')
print("Rabbit MQ connection is established")

for index, row in df.iterrows():
    message = {
               "station_id": row['station_id'],
               "datapoint_id": row['datapoint_id'],
               "alarm_id": row['alarm_id'],
               "value": row['value'],
               "valueThreshold": row['valueThreshold'],
               "isActive": row['isActive'],
               "event_date": row['event_date'],
               "event_time": row['event_time'],
               "event_hour": row['event_hour']
               }

    channel.basic_publish(exchange='', routing_key='bts_in3', body=json.dumps(message))

    print(" [x] Sent 1 line")