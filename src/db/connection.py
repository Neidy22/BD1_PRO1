import pyodbc
from db.config import DRIVER


def newConnection():
    # Establezco la conexión
    # conn = pyodbc.connect(DRIVER, autocommit=True)
    conn = pyodbc.connect(DRIVER)
    # crear cursor  de la conexión
    cursor = conn.cursor()
    return conn, cursor
