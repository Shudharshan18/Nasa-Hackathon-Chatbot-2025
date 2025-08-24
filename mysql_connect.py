import pymysql
from dotenv import load_dotenv
import os
load_dotenv()
try:
    host = os.getenv("DB_HOST")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    database = os.getenv("DB_NAME")
    conn=pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database
    )   
    cursor=conn.cursor()
    cursor.execute("""
                CREATE TABLE fossil_fuel (
                id INT AUTO_INCREMENT PRIMARY KEY,
                year INT NOT NULL,
                emission DOUBLE NOT NULL,
                longitude varchar(255) NOT NULL,
                latitude varchar(255) NOT NULL,
                change_value DOUBLE,
                percentage_change DOUBLE
            );
                """)
    conn.commit()
    print("Successfully connected to the database")
except Exception as e:
    print(e)

def insertion(year,emission,longitude,latitude,change,percentage_change):
    query = "insert into fossil_fuel(year,emission,longitude,latitude,change_value,percentage_change) values(%s,%s,%s,%s,%s,%s)"
    cursor.execute(query,(year,emission,longitude,latitude,change,percentage_change))
    conn.commit()

# def insertion(id,change,percentage_change):
#     query = "insert into fossil_fuel(id,change_value,percentage_change) values(%s,%s,%s)"
#     cursor.execute(query,(id,change,percentage_change))
#     conn.commit()
    
def getdata(s):
    cursor.execute(s)
    return cursor.fetchall()

# print(getdata("SELECT year, emissions FROM fossil_fuel WHERE year BETWEEN 2000 AND 2025"))