# Niraj Bhattarai, Safal Bhandari
import psycopg2
import csv
from urllib.parse import urlparse, uses_netloc
import configparser

#######################################################################
# IMPORTANT:  You must set your config.ini values!
#######################################################################
# The connection string is provided by elephantsql.  Log into your account and copy it into the
# config.ini file.  It should look something like this:
# postgres://vhepsma:Kdcads89DSFlkj23&*dsdc-32njkDSFS@foo.db.elephantsql.com:7812/vhepsma
# Make sure you copy the entire thing, exactly as displayed in your account page!
#######################################################################
config = configparser.ConfigParser()
config.read('config.ini')
connection_string = config['database']['postgres_connection']

#  You may use this in seed_database.  The function reads the CSV files
#  and returns a list of lists.  The first list is a list of classes,
#  the secode list is a list of ships.
def load_seed_data():
    classes = list()
    with open('classes.csv', newline='') as csvfile:
        spamreader = csv.reader(csvfile)
        for row in spamreader:
            classes.append(row)

    ships = list()
    with open('ships.csv', newline='') as csvfile:
        spamreader = csv.reader(csvfile)
        for row in spamreader:
            ships.append(row)
    return [classes, ships]

def create_tables():
    curs = conn.cursor()
    curs.execute("create table Class(Class text NOT NULL, Type text, Country text, NumGuns int, Bore real, Displacement real, PRIMARY KEY(Class))")
    curs.execute("create table Ship(Name text NOT NULL, Class text NOT NULL, Launched int, PRIMARY KEY(Name, Class), FOREIGN KEY(Class) REFERENCES Class(Class) ON DELETE CASCADE ON UPDATE CASCADE)")
    conn.commit()


def insert_data():
    tables = load_seed_data()
    classdata = tables[0]
    shipdata = tables[1]
    curs = conn.cursor()
    for row in classdata:
        curs.execute('insert into Class(Class, Type, Country, NumGuns, Bore, Displacement) values (%s, %s, %s, %s, %s, %s) returning Class', (row[0], row[1], row[2], row[3], row[4], row[5]))
    for row in shipdata:
        curs.execute('insert into Ship(Name, Class, Launched) values (%s, %s, %s) returning Name', (row[0], row[1], row[2]))
    conn.commit()


def delete_tables():
    curs = conn.cursor()
    curs.execute('drop table Class; drop table Ship')
    conn.commit()

def does_tables_exist():
    curs = conn.cursor()
    curs.execute('select exists(select 1 from information_schema.tables where table_schema=(%s) and table_name=(%s))', ('public', 'class'))
    Class = curs.fetchone()
    curs.execute('select exists(select 1 from information_schema.tables where table_schema=(%s) and table_name=(%s))', ('public', 'ship' ))
    Ship = curs.fetchone()
    return(Class[0] and Ship[0])



def seed_database():
    # you must create the necessary tables, if they do not already exist.
    # BE SURE to setup the necessary foreign key constraints such that deleting
    # a class results in all ships of that class being removed automatically.


        # delete_tables(curs)
    # print(does_tables_exist())
    if not does_tables_exist():
        create_tables()
        insert_data()
        # this insures that when tables is created at first there is data in it
        # so I don't feel like there is need to check if table is empty and insert data
         # creates tables if not exists


    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    # Make sure you name your tables and columns EXACTLY as specified
    # in the assignment document.  Failure to do so will result in a minimum
    # of 30 points off your grade!
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    # after ensuring the tables are present, count how many classes there are.
    # if there are none, then call load_data to get the data found in config.json.
    # Insert the data returned from load_data.  Hint - it returns a tuple, with the first being a list
    # of tuples representing classes, and the second being the list of ships.  Carefully
    # examine the code or print the returned data to understand exactly how the data is structured
    # i.e. column orders, etc.

    # If there is already data, there is no need to do anything at all...


# Return list of all classes.  Important, to receive full credit you
# should use a Python generator to yield each item out of the cursor.
# Column order should be class, type, country, guns, bore, displacement
def get_classes():
    curs = conn.cursor()
    curs.execute('select * from Class')

    value = curs.fetchone()
    print(type(value))
    for getaclass in value:
        yield getaclass


# Return list of all ships, joined with class.  Important, to receive full credit you
# should use a Python generator to yield each item out of the cursor.
# Column order should be ship.class, name, launched, class.class, type, country, guns, bore, displacement
# If class_name is not None, only return ships with the given class_name.  Otherwise, return all ships
def get_ships(class_name):
    curs = conn.cursor()
    if class_name==None:
        curs.execute('select * from Ship')
    else:
        curs.execute('select * from Ship join Class on Ship.Class=Class.Class where Class.Class=(%s)', (class_name, ))
    for getaship in curs:
        yield getaship

# Data will be a list ordered with class, type, country, guns, bore, displacement.
def add_class(data):
    curs = conn.cursor()
    curs.execute('insert into Class(Class, Type, Country, NumGuns, Bore, Displacement) values (%s, %s, %s, %s, %s, %s) returning Class', (data[0], data[1], data[2], data[3], data[4], data[5]))
    conn.commit()
# Data will be a list ordered with class, name, launched.
def add_ship(data):
    curs = conn.cursor()
    curs.execute('insert into Ship(Name, Class, Launched) values (%s, %s, %s) returning Name', (data[1], data[0], data[2]))
    conn.commit()
# Delete class with given class name.  Note while there should only be one
# SQL execution, all ships associated with the class should also be deleted.
def delete_class(class_name):
    curs = conn.cursor()
    curs.execute('delete from class where class.class = (%s)', (class_name, ))
    conn.commit()
# Delets the ship with the given class and ship name.
def delete_ship(ship_name, class_name):
    curs = conn.cursor()
    curs.execute('delete from ship where ship.name = (%s) and ship.class = (%s)', (ship_name, class_name, ))
    conn.commit()
def close_connection():
    conn.close()
    return



# This is called at the bottom of this file.  You can re-use this important function in any python program
# that uses psychopg2.  The connection string parameter comes from the config.ini file in this
# particular example.  You do not need to edit this code.
def connect_to_db(conn_str):
    uses_netloc.append("postgres")
    url = urlparse(conn_str)

    conn = psycopg2.connect(database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port)

    return conn

# This establishes the connection, conn will be used across the lifetime of the program.
conn = connect_to_db(connection_string)
seed_database()
