import os
from dotenv import load_dotenv

# Load variables from .env file
load_dotenv()

server = os.getenv('MYSERVER')
dataB = os.getenv('DB')

# ODBC Driver 17 for SQL Server es el que tengo instalado en mi windows
DRIVER = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={dataB};Trusted_Connection=yes;'
print(DRIVER)
