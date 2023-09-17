import pyodbc
from db.script import CREATE_MODEL_SCRIPT, DELETE_MODEL_SCRIPT
from db.connection import connection_to_server


def create_model():
    msg = ""
    try:
        # crear la conexión a la base de datos
        con, curs = connection_to_server()

        # crear la base de datos y las tablas del modelo ya normalizado y con sus respectivas llaves
        sql_model_scripts = CREATE_MODEL_SCRIPT.split(";")
        for script in sql_model_scripts:
            curs.execute(script)

        msg += "Tablas del modelo creadas \n"

        curs.close()  # cierro el cursor
        con.close()  # cierro la conexión
        return f"status: Succesfful, msg: {msg}"
    except pyodbc.Error as e:
        return f"status: Error, msg: {e} "


def delete_model():
    msg = ''
    try:
        conn, curs = connection_to_server()
        sql_model_scripts = DELETE_MODEL_SCRIPT.split(";")
        for script in sql_model_scripts:
            curs.execute(script)
        msg += "Modelo pro1 eliminado"
        curs.close()
        conn.close()
        return f"status: Succesfful, msg: {msg} \n"

    except pyodbc.Error as e:
        return f"status: Error, msg: {e} \n"
