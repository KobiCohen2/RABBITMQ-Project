import pika
import json
from enum import IntEnum
from datetime import datetime
from dbHandler import dbHandler

class Part(IntEnum):
    LOCATION = 0
    TYPE = 1
    NAME = 2

queue_name = 'db-load' #queue name
delimiter = '*' #message tokens delimiter

#open queue channel connection
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

#declare queue
channel.queue_declare(queue=queue_name)

#create dbHandler instance
dbHandler = dbHandler('database.db')

#second queue in order to notify graph_creator on new data
queue_name_db = 'db-load-done'


#A function notifies on new data
def send_message_on_db_load(table_name):
    connection2 = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel2 = connection2.channel()

    channel2.queue_declare(queue=queue_name_db)
    channel2.basic_publish(exchange='', routing_key=queue_name_db, body= 'db load done' + delimiter + table_name)

    connection2.close()


#A callback function receives files to load to DB
def callback(ch, method, properties, body):
    print(body)
    body_tokens = body.split(delimiter) #split body to tokens
    is_succeed = False #flag indicating the success of uploading the data to the database

    #in case of csv file
    if body_tokens[Part.TYPE] == 'csv':
        is_succeed = dbHandler.insert_csv(body_tokens[Part.LOCATION], body_tokens[Part.NAME])

    #in case of json file
    elif body_tokens[Part.TYPE] == 'json':
        is_succeed = dbHandler.insert_json(body_tokens[Part.LOCATION], body_tokens[Part.NAME])

    #in case of unsupported file type
    else:
        print('Unsupported type')
    
    #if succeed notify the graph creator
    if is_succeed:
        send_message_on_db_load(body_tokens[Part.NAME])

channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

#start consuming
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()