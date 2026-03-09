import pymysql

print("Trying to connect...")

try:
    conn = pymysql.connect(
        host="localhost",
        user="root",
        password="password",
        port=3306
    )

    print("Connected successfully!")
    conn.close()

except Exception as e:
    print("Connection failed!")
    print(e)