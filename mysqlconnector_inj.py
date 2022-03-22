import pandas as pd
import numpy as np
import datetime
import mysql.connector
from mysql.connector import errorcode


try:
    cnx = mysql.connector.connect(user='murat', password='im_mysql_tst',
                              host='localhost',
                              database='db_connect')

except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with your user name or password")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exist")
  else:
    print(err)
else:
  cnx.close()

DB_NAME = 'db_connect'

TABLES = {}
TABLES['sync_perc'] = (
    "CREATE TABLE `sync_perc` ("
    "  event_time timestamp NOT NULL ,"
    "  airline	VARCHAR(6) AUTO_INCREMENT,"
    "  airplane	VARCHAR(16) ,"
    "  content_name	VARCHAR(512),"
    "  sync_status	VARCHAR(6),"
    "  content_id	int,"
    "  channel_name	VARCHAR(256),"
    "  content_type	VARCHAR(16),"
    "  file_size	bigint,"
    "  download_percentage	DECIMAL(4,4))"    
    " ENGINE=InnoDB")


cnx = mysql.connector.connect(user='murat', password='im_mysql_tst',
                              host='localhost',
                              database='db_connect')
cursor = cnx.cursor()


for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

cursor.close()
cnx.close()



data  = pd.read_csv('CAL.csv')
print(data)


data.iloc[:,0] = pd.to_datetime(data.iloc[:,0])
data.loc[:,'download_perc'] = data.loc[:,'download_perc'].astype(float)


