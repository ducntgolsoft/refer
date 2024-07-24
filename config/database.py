import mysql.connector
import os
from dotenv import load_dotenv
load_dotenv()

def conn():
    try:
        return mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            user=os.getenv('DB_USERNAME'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_DATABASE')
        )
    except mysql.connector.Error as err:
        print(f"Lỗi kết nối cơ sở dữ liệu: {err}")
        exit(1)


def conn_live():
    try:
        return mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            user=os.getenv('DB_USERNAME'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_DATABASE')
        )
    except mysql.connector.Error as err:
        print(f"Lỗi kết nối cơ sở dữ liệu: {err}")
        exit(1)


db_connection = conn()
db_connection_live = conn_live()
