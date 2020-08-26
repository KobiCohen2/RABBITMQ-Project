from dbHandler import dbHandler
import os

#run command: pytest tests.py

# Test #1 - insert_csv function
#assumption: invoices_2012 exists and empty
def test_insert_csv():
    #create dbHandler instance
    dbHandler_ = dbHandler('database.db')

    return_value = dbHandler_.insert_csv(os.path.abspath(os.getcwd()), 'invoices_2012.csv')
    result_set = dbHandler_.select('SELECT * FROM invoices_2012')

    assert return_value == True and len(result_set) > 0 

# Test #2 - insert_json function
#assumption: invoices_2009 exists and empty
def test_insert_json():
    #create dbHandler instance
    dbHandler_ = dbHandler('database.db')

    return_value = dbHandler_.insert_json(os.path.abspath(os.getcwd()), 'invoices_2009.json')
    result_set = dbHandler_.select('SELECT * FROM invoices_2009')

    assert return_value == True and len(result_set) > 0 