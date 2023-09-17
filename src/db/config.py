import os
from dotenv import load_dotenv

# Load variables from .env file
load_dotenv()

server = os.getenv('MYSERVER')
dataB = os.getenv('DB')

# ODBC Driver 17 for SQL Server es el que tengo instalado en mi windows
DRIVER_DATA = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={dataB};Trusted_Connection=yes;'

# ODBC Driver 17 for SQL Server es el que tengo instalado en mi windows
DRIVER_SERVER = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};Trusted_Connection=yes;'
# print(DRIVER)
