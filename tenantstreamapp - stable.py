import pika, sys, os, json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import collect_list, avg, min, max, row_number, col
from pyspark.sql.window import Window
from utils import sensor_names, sensors, alarms


### Section of analyzer functions for calculating min, max and average
def main_analyzer(df, windowSpec):
    df_temp = df.withColumn("avg", avg('value').over(windowSpec)). \
        withColumn("min", min('value').over(windowSpec)). \
        withColumn("max", max('value').over(windowSpec)). \
        dropDuplicates(["event_date", "datapoint_id", "event_hour"])

    print("Streaming analytics is completed")

    return (df_temp.select(col("datapoint_id"), \
                           col("event_date"), col("event_hour"), \
                           col("avg"), col("min"), col("max")))


#Publisher function to send the message to the outgoing queue
def publisher(df, sensor_names, channel):
    messages = df.toPandas()

    for index, row in messages.iterrows():
        message = {"sensor_name": sensor_names[row['datapoint_id']],
                   "event_date": row['event_date'],
                   "event_hour": row['event_hour'],
                   "avg": row['avg'],
                   "min": row['min'],
                   "max": row['max']
                   }


        channel.basic_publish(exchange='', routing_key='streaming_analytics_out', body=json.dumps(message))

        print(" [x] Sent 1 line")


def callback(ch, method, properties, body):

    #Get global variables

    global current_hour
    global windowSpec

    #Receive the data from JSON message

    row = json.loads(body.decode('utf8').replace("'", '"'))

    #Check if the data is for the next hour. If not, run hourly analytics and clear the current hour list

    if row['event_hour'] > current_hour or row['event_hour'] == 0:
        print(current_hour)
        df = pd.DataFrame(columns=['station_id', 'datapoint_id', 'alarm_id', 'value', 'valueThreshold', 'isActive',
                                   'event_date', 'event_time', 'event_hour'])
        for m in message_window:
            df = df.append(m, ignore_index=True)

        #Run hourly analytics
        if(len(message_window) > 0):
            df_spark = spark.createDataFrame(data=df)
            print("Spark DF is created")
            res = main_analyzer(df_spark, windowSpec)
            publisher(res, sensor_names, channel)

        #Change the hour and clear the list of messages
        current_hour = row['event_hour']
        message_window.clear()


    #If the alarm was active and there were no same alarms sent this hour, send an alarm
    if row['isActive']:
        timestamp = row['event_date'] + " " + str(row['event_hour']) + ":00"

        if alarms[row['alarm_id']] not in alarms_sent.keys():
            alarms_sent[alarms[row['alarm_id']]] = [timestamp]
            message = {"alarm": alarms[row['alarm_id']], "timestamp": timestamp}
            channel.basic_publish(exchange='', routing_key='alarms_out', body=json.dumps(message))

        elif timestamp not in alarms_sent[alarms[row['alarm_id']]]:
            alarms_sent[alarms[row['alarm_id']]].append(timestamp)
            message = {"alarm": alarms[row['alarm_id']], "timestamp": timestamp}
            channel.basic_publish(exchange='', routing_key='alarms_out', body=json.dumps(message))

    message_window.append(row)


if __name__ == '__main__':
    #Iitial variables for message exchange
    alarms_sent = {}
    message_window = []
    current_hour = 0

    #Spark setup
    spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
    windowSpec = Window.partitionBy("event_date", "datapoint_id", "event_hour")
    print("Spark session is created")

    #Establish RabbitMQ connection
    connection = pika.BlockingConnection(pika.ConnectionParameters(heartbeat=100, host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='bts_in3')
    print("RabbitMQ connection is established")

    #Start consuming
    channel.basic_consume(queue='bts_in3', on_message_callback=callback, auto_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

