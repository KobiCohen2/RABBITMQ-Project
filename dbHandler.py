import sqlite3
import csv
import json

insert_statement_prefix = 'INSERT INTO '
insert_statement_postfix = ' (BillingAddress, BillingCity, BillingCountry, BillingPostalCode, BillingState,CustomerId,InvoiceDate, InvoiceId, Total) VALUES (?,?,?,?,?,?,?,?,?)'

class dbHandler:
    def __init__(self,dbPath):
        self.path=dbPath
        self.conn = None

    def getPath(self):
        return self.path

    def connect(self):
        try:
            conn = sqlite3.connect(self.path)
            conn.text_factory = str
            self.conn = conn#.cursor()
            return self.conn
        except :
            print ('connection error')
            return -1

    def disconnect(self):
        if self.conn:
            self.conn.commit()
            self.conn.close()
            print("disconnected from database")

    def select(self, query):
        cur = self.connect().cursor()
        cur.execute(query)
        rs = cur.fetchall()
        self.disconnect()
        return rs


    #A function inserts csv data file into the database
    def insert_csv(self, location, file_name):
        csvData = csv.reader(open(location + '\\' + file_name, "rb"), delimiter=',') #read file content
        table_name = file_name.split('.')[0] #extract table name
        insert_statement = insert_statement_prefix + table_name + insert_statement_postfix
        i = 0
        data = []
        for row in csvData:
            if i == 0:
                i += 1
                continue #skip the header line
            data.append((row[3], row[4], row[6], row[7], row[5], int(row[1]), row[2], int(row[0]), float(row[8])))

        try:
            cur = self.connect().cursor() #get cursor
            cur.executemany(insert_statement, data) #execute query
        except:
            return False #return false on failure
        finally:
            self.disconnect() #disconnect
        return True #return true on success
    
    #A function inserts json data file into the database
    def insert_json(self, location, file_name):
        table_name = file_name.split('.')[0] #extract table name
        insert_statement = insert_statement_prefix + table_name + insert_statement_postfix

        with open(location + '\\' + file_name) as f:
            data = json.load(f) #load json data file

        data_to_insert = []
        for line in data:
            data_to_insert.append((line['BillingAddress'],
            line['BillingCity'], line['BillingCountry'],
            line['BillingPostalCode'], line['BillingState'],
            int(line['CustomerId']), line['InvoiceDate'],
            int(line['InvoiceId']), float(line['Total'])))

        try:
            cur = self.connect().cursor() #get cursor
            cur.executemany(insert_statement, data_to_insert) #execute query
        except:
            return False #return false on failure
        finally:
            self.disconnect() #disconnect
        return True #return true on success





