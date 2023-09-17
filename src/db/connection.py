import pyodbc
from db.config import DRIVER_DATA, DRIVER_SERVER


def connection_to_server():
    # Establezco la conexión
    # conn = pyodbc.connect(DRIVER, autocommit=True)
    conn = pyodbc.connect(DRIVER_SERVER, autocommit=True)
    # crear cursor  de la conexión
    cursor = conn.cursor()
    return conn, cursor


def connection_to_database():
    # Establezco la conexión
    # conn = pyodbc.connect(DRIVER, autocommit=True)
    conn = pyodbc.connect(DRIVER_DATA, autocommit=True)
    # crear cursor  de la conexión
    cursor = conn.cursor()
    return conn, cursor
