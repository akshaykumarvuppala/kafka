import datetime
import random
import time
import string
import mysql.connector

# MySQL connection configuration
mysql_host = 'localhost'  
mysql_user = 'root'  
mysql_password = 'Vvakshay1@$'  
mysql_database = 'akshay' 

# Connect to MySQL
cnx = mysql.connector.connect(
    host=mysql_host,
    user=mysql_user,
    password=mysql_password,
    database=mysql_database
)
cursor = cnx.cursor()
print(cursor)

list1=[('Ananth','India'),('john','USA'),('anglo','africa'),('uno','canada'),('curran','zimbambe')]

# Insert 4-5 dummy/dynamically prepared records in MySQL table
for i in range(len(list1)):
    # Generate random values for the record
    id = i+1
    name = list1[i][0]
    age = random.randint(18, 60)

    country = list1[i][1]
    created_at = datetime.datetime.now()


    

    # Insert the record into the MySQL table
    cursor.execute(f"INSERT INTO mytable (id, name, age, country, created_at) "
                   f"VALUES ({id}, '{name}', {age}, '{country}', '{created_at}')")
    
    
    cnx.commit()
    time.sleep(10)

# Close MySQL connection
cursor.close()
cnx.close()
