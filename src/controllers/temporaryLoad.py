import pyodbc
from db.connection import newConnection
import csv
import os

HERE = os.path.dirname(os.path.abspath(__file__))
# crear el script de las tablas temporales, estas no contienen llaves
script_temp = """
    CREATE TABLE #ciudadano_temp
    (
        dpi VARCHAR (13) NOT NULL,
        nombre VARCHAR (50) NOT NULL,
        apellido VARCHAR (50) NOT NULL,
        direccion VARCHAR (100) NOT NULL,
        telefono VARCHAR (10) NOT NULL,
        edad INT NOT NULL,
        genero VARCHAR (1)

    );

    CREATE TABLE #departamento_temp
    (
        id_departamento INT NOT NULL,
        nombre VARCHAR (50) NOT NULL
    );

    CREATE TABLE #partido_temp
    (
        id_partido INT NOT NULL,
        nombre VARCHAR (100) NOT NULL,
        siglas VARCHAR (50) NOT NULL,
        fecha_fundacion DATE NOT NULL
    );

    CREATE TABLE #cargo_temp
    (
        id_cargo INT NOT NULL,
        cargo VARCHAR (100) NOT NULL,
    );

    CREATE TABLE #mesa_temp
    (
        id_mesa INT NOT NULL,
        id_departamento INT NOT NULL
    );

    CREATE TABLE #candidato_temp
    (
        id_candidato INT NOT NULL,
        nombres VARCHAR (150) NOT NULL,
        fecha_nacimiento DATE NOT NULL,
        id_partido INT NOT NULL,
        id_cargo INT NOT NULL
    );

    CREATE TABLE #voto_temp
    (
        id_voto INT NOT NULL,
        id_candidato INT NOT NULL,
        dpi VARCHAR (13) NOT NULL,
        id_mesa INT NOT NULL,
        fecha DATE NOT NULL,
        hora TIME NOT NULL,
    );
"""


def temporary_bulk_upload():
    msg = ""
    try:
        # crear la conexión a la base de datos
        con, curs = newConnection()
        msg += "Conexión exitosa \n"
        # obtener los scripts de las creaciones de tablas
        sql_temp_scripts = script_temp.split(";")
        # recorrer el arreglo de scripts y crear las tablas
        for script in sql_temp_scripts:
            curs.execute(script)
            # msg += "Se creó la tabla temporal \n"
        msg += "Tablas temporales creadas \n"
        # llenar las tablas temporales
        msg += load_tables_from_files(con, curs)

        # crear las tablas del modelo ya normalizado y con sus respectivas llaves

        # llenar el modelo
        con.commit()
        curs.close()  # cierro el cursor
        con.close()  # cierro la conexión

    except pyodbc.Error as e:
        print('Error en carga masiva: ', e)
        msg = f"Error en carga masiva {e}"
    return msg


def load_tables_from_files(conn, cursor):
    msg = ''
    msg += load_citizen_temp(conn, cursor)
    msg += load_department_temp(conn, cursor)
    msg += load_political_temp(conn, cursor)
    msg += load_position_temp(conn, cursor)
    msg += load_station_temp(conn, cursor)
    msg += load_candidate_temp(conn, cursor)
    msg += load_vote_temp(conn, cursor)

    return msg


def load_citizen_temp(conn, cursor):
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
                cursor.execute(sql, (dpi, name, last_name,
                               direction, telephone, age, gender))
                # Guardar los cambios
                # conn.commit()
            txt += "Tabla temporal Ciudadano cargada con éxito \n"
    except pyodbc.Error as e:
        txt = f"No se cargaron los datos de la tabla temporal ciudadano, {e} \n"
    return txt


def load_department_temp(conn, cursor):
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
                cursor.execute(sql, (id, name))
                # Guardar los cambios
                # conn.commit()
            txt += "Tabla temporal Departamento cargada con éxito \n"
    except pyodbc.Error as e:
        txt = f"No se cargaron los datos de la tabla temporal departamento, {e} \n"
    return txt


def load_political_temp(conn, cursor):
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
                cursor.execute(sql, (id, name, acronym, date))
                # Guardar los cambios
                # conn.commit()
            txt += "Tabla temporal Partido cargada con éxito \n"
    except pyodbc.Error as e:
        txt = f"No se cargaron los datos de la tabla temporal partido, {e} \n"
    return txt


def load_position_temp(conn, cursor):
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
                cursor.execute(sql, (id, position))
                # Guardar los cambios
                # conn.commit()
            txt += "Tabla temporal Cargo cargada con éxito \n"
    except pyodbc.Error as e:
        txt = f"No se cargaron los datos de la tabla temporal cargo, {e} \n"
    return txt


def load_station_temp(conn, cursor):
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
                cursor.execute(sql, (id_station, id_department))
                # Guardar los cambios
                # conn.commit()
            txt += "Tabla temporal Mesa cargada con éxito \n"
    except pyodbc.Error as e:
        txt = "No se cargaron los datos de la tabla temporal mesa, {e} \n"
    return txt


def load_candidate_temp(conn, cursor):
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
                cursor.execute(sql, (id, names, birth_date,
                               id_political, id_position))
                # Guardar los cambios
                # conn.commit()
            txt += "Tabla temporal Candidato cargada con éxito \n"
    except pyodbc.Error as e:
        txt = f"No se cargaron los datos de la tabla temporal candidato, {e} \n"
    return txt


def load_vote_temp(conn, cursor):
    txt = ''
    file_path = os.path.join(HERE, 'votaciones.csv')
    try:
        with open(file_path, encoding='utf-8') as my_file:
            reader = csv.reader(my_file)
            rows = list(reader)
            rows = rows[1:]  # quito la fila del encabezado del archivo
            sql = 'INSERT INTO #voto_temp (id_voto, id_candidato, dpi, id_mesa, fecha, hora) VALUES(?,?,?,?,?,?);'
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

                cursor.execute(sql, (id_vote, id_candidate,
                               dpi, id_station, date, time))
                # Guardar los cambios
                # conn.commit()
            txt += "Tabla temporal Voto cargada con éxito \n"
    except pyodbc.Error or UnicodeDecodeError or os.error as e:

        txt = f"No se cargaron los datos de la tabla temporal voto, {e} \n"
    return txt
