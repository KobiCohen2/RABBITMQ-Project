import pika
import os
import time

directory = os.path.abspath(os.getcwd()) #get current working directory
queue_name = 'db-load' #queue name
delimiter = '*' #message tokens delimiter

#open queue channel connection
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

#declare queue
channel.queue_declare(queue=queue_name)

#loop through all data files
#create messages and send to queue
for filename in os.listdir(directory):
    if filename.endswith(".csv"): 
         channel.basic_publish(exchange='', routing_key=queue_name, body= directory + delimiter + 'csv' + delimiter + os.path.basename(filename))
    elif filename.endswith(".json"):
        channel.basic_publish(exchange='', routing_key=queue_name, body= directory + delimiter + 'json' + delimiter + os.path.basename(filename))
    time.sleep(5)

#close the connection
connection.close()
