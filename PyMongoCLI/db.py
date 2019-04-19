# Niraj Bhattarai
import pymongo
from pymongo import MongoClient
import csv
import configparser

#######################################################################
# IMPORTANT:  You must set your config.ini values!
#######################################################################
# The connection string is provided by mlab.  Log into your account and copy it into the
# config.ini file.  It should look something like this:
# mongodb://labs:test@ds213239.mlab.com:13239/cmps364
# Make sure you copy the entire thing, exactly as displayed in your account page!
#
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Please make sure your database is named cmps 364
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#
#######################################################################
config = configparser.ConfigParser()
config.read('config.ini')
connection_string = config['database']['mongo_connection']

db= None
ships = None
classes = None

def list_to_dict(keys, list):
    document = dict()
    count = 0
    for key in keys:
        document[key] = list[count]
        count = count + 1
    return document

def load_data():
    with open('classes.csv', newline='') as csvfile:
        spamreader = csv.reader(csvfile)
        for row in spamreader:
            add_class(row)

    with open('ships.csv', newline='') as csvfile:
        spamreader = csv.reader(csvfile)
        for row in spamreader:
            add_ship(row)


def seed_database():
    existing = classes.find({})
    if existing.count() < 1:
        data = load_data()
        # insert the data....

    # If there is already data, there is no need to do anything at all...

# utility lists defining the fields, and the order they are expected to be in by ui.py
class_keys = ('class', 'type', 'country', 'guns', 'bore', 'displacement')
ship_keys = ('name', 'class', 'launched')


# utility function you might find useful.. accepts a key list (see above) and
# a document returned by pymongo (dictionary) and turns it into a list.
def to_list(keys, document):
    record = []
    for key in keys:
        record.append(document[key])
    return record

# utility function you might find useful...  Similar to to_list above, but it's appending
# to a list (record) instead of creating a new one.  Useful for when you already have a
# list, but need to join another dictionary object into it...
def join(keys, document, record):
    for key in keys:
        record.append(document[key])
    return record


# Return list of all classes.  Important, to receive full credit you
# should use a Python generator to yield each item out of the cursor.
# Field order should be class, type, country, guns, bore, displacement
# -- Remember, pymongo returns a dictionary, so you need to transform it into a list!
def get_classes():
    global classes
    for document in classes.find({}):
        yield to_list(class_keys, document)

def join_ship_class(ship_document, class_document, class_ship_keys):
    document = dict()
    document[class_ship_keys[0]] = ship_document['class']
    document[class_ship_keys[1]] = ship_document['name']
    document[class_ship_keys[2]] = ship_document['launched']
    document[class_ship_keys[3]] = class_document['class']
    document[class_ship_keys[4]] = class_document['type']
    document[class_ship_keys[5]] = class_document['country']
    document[class_ship_keys[6]] = class_document['guns']
    document[class_ship_keys[7]] = class_document['bore']
    document[class_ship_keys[8]] = class_document['displacement']
    return document

# Return list of all ships, joined with class.  Important, to receive full credit you
# should use a Python generator to yield each item out of the cursor.
# Field order should be ship.class, name, launched, class.class, type, country, guns, bore, displacement
# If class_name is not None, only return ships with the given class_name.  Otherwise, return all ships
def get_ships(class_name):
    global ships
    global classes
    class_ship_keys = ('ship.class', 'name', 'launched', 'class.class', 'type', 'country', 'guns', 'bore', 'displacement')
    if class_name==None:
        for document in ships.find({}):
            result = classes.find_one({'class' : document['class']})
            yield to_list(class_ship_keys, join_ship_class(document, result, class_ship_keys))

    else:
        for document in ships.find({'class' : class_name}):
            result = classes.find_one({'class' : class_name})
            yield to_list(class_ship_keys, join_ship_class(document, result, class_ship_keys))

# Data will be a list ordered with class, type, country, guns, bore, displacement.
def add_class(data):
    global classes
    classes.insert(list_to_dict(class_keys, data))

# Data will be a list ordered with class, name, launched.
def add_ship(data):
    global ships
    ships.insert(list_to_dict(ship_keys, data))


def delete_class(class_name):
    global classes
    classes.delete_one({'class': class_name}) # rn deletes class_name that comes up first
    ships_related_to_class = ships.delete_many({'class': class_name})

def delete_ship(ship_name, class_name):
    global ships
    ships.delete_one({'name': ship_name, 'class': class_name})

# This is called at the bottom of this file.  You can re-use this important function in any python program
# that uses mongodb.  The connection string parameter comes from the config.json file in this
# particular example.

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Please make sure your database is named cmps 364
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
def connect_to_db(conn_str):
    global classes
    global ships
    global db
    client = MongoClient(conn_str)
    db = client.cmps364
    classes = client.cmps364.classes
    ships = client.cmps364.ships
    return client

# This establishes the connection, conn will be used across the lifetime of the program.
conn = connect_to_db(connection_string)
seed_database()
