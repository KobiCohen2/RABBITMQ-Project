Prerequisites
1. pip install pika
2. pip install plotly
3. pip install enum
4. pip install -U pytest

3 Main Modules:
1. producer - Sends to a rabbitmq queue message containing file location, file type (CSV or JSON).
There is a table in the database to which the files should be uploaded.
2. consumer - Will listen to the queue and as soon as it receives the message it will upload the file to the appropriate table in a database. After uploading the file to the table, the system sends to an additional queue notification when the upload is complete.
3. graph_creator - Will listen to the additional queue and upon receiving the message will display the following data in graphs that are updated in real-time:
1. Total sales per month each year.
2. The number of active customers per month each year.

Running Instructions
1. Run the second module - python consumer.py
2. Run the third module - python graph_creator.py
3. Run the first module - python producer.py

Tests
In order to run the tests - pytest tests.py
Assumption: the database table exists and empty