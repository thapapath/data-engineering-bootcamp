import csv
import configparser

import psycopg2


parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("postgres_config", "database")
user = parser.get("postgres_config", "username")
password = parser.get("postgres_config", "password")
host = parser.get("postgres_config", "host")
port = parser.get("postgres_config", "port")

conn_str = f"dbname={dbname} user={user} password={password} host={host} port={port}"
conn = psycopg2.connect(conn_str)
cursor = conn.cursor()

DATA_FOLDER = "data"

table = "addresses"
header = ["address_id", "address", "zipcode", "state", "country"]

with open(f"{DATA_FOLDER}/order_items.csv", "w", newline='') as f:  
    writer = csv.writer(f)
    writer.writerow(header)  

    query = f"SELECT * FROM {table}"
    cursor.execute(query)

    results = cursor.fetchall()

    for each in results:
        writer.writerow(each)

cursor.close()
conn.close()