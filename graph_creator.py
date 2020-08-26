import pika
import plotly.graph_objects as go
import plotly
from dbHandler import dbHandler

queue_name_db = 'db-load-done' #queue name
delimiter = '*' #message tokens delimiter

dates1 = [] #X
total = [] #Y

dates2 = [] #X
customers = [] #Y

#open queue channel connection
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

#declare queue
channel.queue_declare(queue=queue_name_db)

#create dbHandler instance
dbHandler = dbHandler('database.db')

#A callback function receives notifications on new data
def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    table_name = body.split(delimiter)[1].split('.')[0] #extract table name

    #construct queries
    query1_prefix = 'SELECT SUM(Total) as Price, strftime("%m-%Y", InvoiceDate) as \'month-year\' FROM ' 
    query1_postfix = ' GROUP BY strftime("%m-%Y", InvoiceDate);'

    query2_prefix = 'SELECT COUNT (DISTINCT CustomerId), strftime("%m-%Y", InvoiceDate) as \'month-year\' FROM ' 
    query2_postfix = ' GROUP BY strftime("%m-%Y", InvoiceDate);'

    #execute queries
    rs1 = dbHandler.select(query1_prefix + table_name + query1_postfix)
    rs2 = dbHandler.select(query2_prefix + table_name + query2_postfix)

    #collect data
    for row in rs1:
        total.append(row[0])
        dates1.append(row[1])
    
    for row in rs2:
        customers.append(row[0])
        dates2.append(row[1])
  
    #create graphs
    plotly.offline.plot({
    "data": [go.Scatter(x=dates1, y=total)],
        "layout": go.Layout(title="Total sales per month each year")}, auto_open=True, filename='graph1.html')
    
    plotly.offline.plot({
    "data": [go.Scatter(x=dates2, y=customers)],
        "layout": go.Layout(title="Number of active customers per month each year")}, auto_open=True, filename='graph2.html')

channel.basic_consume(
    queue=queue_name_db, on_message_callback=callback, auto_ack=True)

#start consuming
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()