import pyodbc
from db.connection import connection_to_server, connection_to_database
import csv
import os
from db.script import TEMP_SCRIPT
from controllers.model import create_model, delete_model

HERE = os.path.dirname(os.path.abspath(__file__))
# crear el script de las tablas temporales, estas no contienen llaves


def temporary_bulk_upload():
    msg = ""
    try:
        # crear la base de datos y las tablas del modelo ya normalizado y con sus respectivas llaves

        msg += create_model()

        # usar la nueva base
        conn, curs = connection_to_database()

        # obtener los scripts de las creaciones de tablas
        sql_temp_scripts = TEMP_SCRIPT.split(";")
        # recorrer el arreglo de scripts y crear las tablas
        for script in sql_temp_scripts:
            curs.execute(script)
            # msg += "Se creó la tabla temporal \n"
        msg += "Tablas temporales creadas \n"
        # llenar las tablas temporales
        msg += load_tables_from_files(curs)

        # con.commit()
        curs.close()  # cierro el curs
        conn.close()  # cierro la conexión

    except pyodbc.Error as e:
        msg = f"Error en carga masiva {e}"
    return msg


def load_tables_from_files(curs):
    msg = ''
    msg += load_citizen_temp(curs)
    msg += load_department_temp(curs)
    msg += load_political_temp(curs)
    msg += load_position_temp(curs)
    msg += load_station_temp(curs)
    msg += load_candidate_temp(curs)
    msg += load_vote_temp(curs)

    return msg


def load_citizen_temp(curs):
    txt = ''
    file_path = os.path.join(HERE, 'ciudadanos.csv')
    try:
        with open(file_path, encoding='utf-8') as my_file:
            reader = csv.reader(my_file)
            rows = list(reader)
            rows = rows[1:]  # quito la fila del encabezado del archivo
            sql = 'INSERT INTO #ciudadano_temp (dpi, nombre, apellido, direccion, telefono, edad, genero) VALUES(?,?,?,?,?,?,?);'
            for row in rows:
                dpi = row[0]
                name = row[1]
                last_name = row[2]
                direction = row[3]
                telephone = row[4]
                age = row[5]
                gender = row[6]
                curs.execute(sql, (dpi, name, last_name,
                                   direction, telephone, age, gender))
                # Guardar los cambios
                # conn.commit()
            txt += "Tabla temporal Ciudadano cargada con éxito \n"
    except pyodbc.Error as e:
        txt = f"No se cargaron los datos de la tabla temporal ciudadano, {e} \n"
    return txt


def load_department_temp(curs):
    txt = ''
    file_path = os.path.join(HERE, 'departamentos.csv')
    try:
        with open(file_path, encoding='utf-8') as my_file:
            reader = csv.reader(my_file)
            rows = list(reader)
            rows = rows[1:]  # quito la fila del encabezado del archivo
            sql = 'INSERT INTO #departamento_temp (id_departamento, nombre) VALUES(?,?);'
            for row in rows:
                id = row[0]
                name = row[1]
                curs.execute(sql, (id, name))
                # Guardar los cambios
                # conn.commit()
            txt += "Tabla temporal Departamento cargada con éxito \n"
    except pyodbc.Error as e:
        txt = f"No se cargaron los datos de la tabla temporal departamento, {e} \n"
    return txt


def load_political_temp(curs):
    txt = ''
    file_path = os.path.join(HERE, 'partidos.csv')
    try:
        with open(file_path, encoding='utf-8') as my_file:
            reader = csv.reader(my_file)
            rows = list(reader)
            rows = rows[1:]  # quito la fila del encabezado del archivo
            sql = 'INSERT INTO #partido_temp (id_partido, nombre, siglas, fecha_fundacion) VALUES(?,?,?,?);'

            for row in rows:
                id = row[0]
                name = row[1]
                acronym = row[2]
                # separando la fecha para cambiarle la sintaxis a la esperada por sql server
                ddmmyy = row[3].split('/')
                dd = ddmmyy[0]
                mm = ddmmyy[1]
                yy = ddmmyy[2]
                date = f'{yy}-{mm}-{dd}'
                curs.execute(sql, (id, name, acronym, date))
                # Guardar los cambios
                # conn.commit()
            txt += "Tabla temporal Partido cargada con éxito \n"
    except pyodbc.Error as e:
        txt = f"No se cargaron los datos de la tabla temporal partido, {e} \n"
    return txt


def load_position_temp(curs):
    txt = ''
    file_path = os.path.join(HERE, 'cargos.csv')
    try:
        with open(file_path, encoding='utf-8') as my_file:
            reader = csv.reader(my_file)
            rows = list(reader)
            rows = rows[1:]  # quito la fila del encabezado del archivo
            sql = 'INSERT INTO #cargo_temp (id_cargo, cargo) VALUES(?,?);'
            for row in rows:
                id = row[0]
                position = row[1]
                curs.execute(sql, (id, position))
                # Guardar los cambios
                # conn.commit()
            txt += "Tabla temporal Cargo cargada con éxito \n"
    except pyodbc.Error as e:
        txt = f"No se cargaron los datos de la tabla temporal cargo, {e} \n"
    return txt


def load_station_temp(curs):
    txt = ''
    file_path = os.path.join(HERE, 'mesas.csv')
    try:
        with open(file_path, encoding='utf-8') as my_file:
            reader = csv.reader(my_file)
            rows = list(reader)
            rows = rows[1:]  # quito la fila del encabezado del archivo
            sql = 'INSERT INTO #mesa_temp (id_mesa, id_departamento) VALUES(?,?);'
            for row in rows:
                id_station = row[0]
                id_department = row[1]
                curs.execute(sql, (id_station, id_department))
                # Guardar los cambios
                # conn.commit()
            txt += "Tabla temporal Mesa cargada con éxito \n"
    except pyodbc.Error as e:
        txt = "No se cargaron los datos de la tabla temporal mesa, {e} \n"
    return txt


def load_candidate_temp(curs):
    txt = ''
    file_path = os.path.join(HERE, 'candidatos.csv')
    try:
        with open(file_path, encoding='utf-8') as my_file:
            reader = csv.reader(my_file)
            rows = list(reader)
            rows = rows[1:]  # quito la fila del encabezado del archivo
            sql = 'INSERT INTO #candidato_temp (id_candidato, nombres, fecha_nacimiento, id_partido, id_cargo) VALUES(?,?,?,?,?);'
            for row in rows:
                id = row[0]
                names = row[1]
                # separando la fecha para cambiarle la sintaxis a la esperada por sql server
                ddmmyy = row[2].split('/')
                dd = ddmmyy[0]
                mm = ddmmyy[1]
                yy = ddmmyy[2]
                birth_date = f'{yy}-{mm}-{dd}'
                id_political = row[3]
                id_position = row[4]
                curs.execute(sql, (id, names, birth_date,
                                   id_political, id_position))
                # Guardar los cambios
                # conn.commit()
            txt += "Tabla temporal Candidato cargada con éxito \n"
    except pyodbc.Error as e:
        txt = f"No se cargaron los datos de la tabla temporal candidato, {e} \n"
    return txt


def load_vote_temp(curs):
    txt = ''
    file_path = os.path.join(HERE, 'votaciones.csv')
    try:
        with open(file_path, encoding='utf-8') as my_file:
            reader = csv.reader(my_file)
            rows = list(reader)
            rows = rows[1:]  # quito la fila del encabezado del archivo
            sql = 'INSERT INTO #voto_temp (id_voto, id_candidato, dpi, id_mesa, fecha_hora) VALUES(?,?,?,?,?);'
            for row in rows:
                id_vote = row[0]
                id_candidate = row[1]
                dpi = row[2]
                id_station = row[3]
                # columna que trae la fecha y hora juntas, las separo
                date_time = row[4].split(" ")
                # separando la fecha para cambiarle la sintaxis a la esperada por sql server
                ddmmyy = date_time[0].split('/')
                dd = ddmmyy[0]
                mm = ddmmyy[1]
                yy = ddmmyy[2]
                date = f'{yy}-{mm}-{dd}'

                time = date_time[1]
                date_time = f'{date} {time}'

                curs.execute(sql, (id_vote, id_candidate,
                                   dpi, id_station, date_time))
                # Guardar los cambios
                # conn.commit()
            txt += "Tabla temporal Voto cargada con éxito \n"
    except pyodbc.Error or UnicodeDecodeError or os.error as e:

        txt = f"No se cargaron los datos de la tabla temporal voto, {e} \n"
    return txt


def drop_temporary_and_model():
    msg = ''
    conn, curs = connection_to_server()
    # Eliminar el modelo
    msg += delete_model()
    # Eliminar las tablas temporales
    curs.close()
    conn.close()

    msg += "Tablas temporales eliminadas \n"
    return msg
