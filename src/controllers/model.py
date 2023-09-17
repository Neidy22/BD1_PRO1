import pyodbc
from db.script import CREATE_MODEL_SCRIPT, DELETE_MODEL_SCRIPT
from db.connection import connection_to_server, connection_to_database
import json


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


def bulk_model(conn, curs):
    msg = ''
    try:
        sql_load = "INSERT INTO ciudadano (dpi, nombre, apellido, direccion, telefono, edad, genero) SELECT dpi, nombre, apellido, direccion, telefono, edad, genero FROM #ciudadano_temp"
        curs.execute(sql_load)
        msg += "Tabla ciudadanos cargada con éxito \n"

        sql_load = "INSERT INTO departamento (id_departamento, nombre) SELECT id_departamento, nombre FROM #departamento_temp"
        curs.execute(sql_load)
        msg += "Tabla departamentos cargada con éxito \n"

        sql_load = "INSERT INTO partido (id_partido, nombre, siglas, fecha_fundacion) SELECT id_partido, nombre, siglas, fecha_fundacion FROM #partido_temp"
        curs.execute(sql_load)
        msg += "Tabla partidos cargada con éxito \n"

        sql_load = "INSERT INTO cargo (id_cargo, cargo) SELECT id_cargo, cargo FROM #cargo_temp"
        curs.execute(sql_load)
        msg += "Tabla cargos cargada con éxito \n"

        sql_load = "INSERT INTO mesa (id_mesa, id_departamento) SELECT id_mesa, id_departamento FROM #mesa_temp"
        curs.execute(sql_load)
        msg += "Tabla mesas cargada con éxito \n"

        sql_load = "INSERT INTO candidato (id_candidato, nombres, fecha_nacimiento, id_partido, id_cargo) SELECT id_candidato, nombres, fecha_nacimiento, id_partido, id_cargo FROM #candidato_temp"
        curs.execute(sql_load)
        msg += "Tabla candidatos cargada con éxito \n"

        # cargar la tabla de detalle de votos con todos los votos
        sql_load = "INSERT INTO voto (id_voto, dpi, id_mesa, fecha_hora) SELECT id_voto, dpi, id_mesa, fecha_hora FROM #voto_temp GROUP BY id_voto, dpi, id_mesa, fecha_hora"
        curs.execute(sql_load)
        msg += "Tabla votos cargada con éxito \n"

        sql_load = "INSERT INTO detalle_voto (id_voto, id_candidato) SELECT id_voto, id_candidato FROM #voto_temp"
        curs.execute(sql_load)
        msg += "Tabla detalle votos cargada con éxito \n"

        # curs.execute('SELECT * FROM voto')
        # rows = curs.fetchall()
        # for row in rows:
        #     print(row.id_voto, row.dpi, row.id_mesa, row.fecha_hora, '\n')

        return f"status: Succesfful, msg: {msg}"

    except pyodbc.Error as e:
        return f"status: Error, msg: {e} "


def consulta1():
    '''
    Mostrar el nombre de los candidatos a presidentes y vicepresidentes por partido (en 
    este reporte/consulta se espera ver tres columnas: “nombre presidente”, “nombre 
    vicepresidente”, “partido”)
    '''

    conn, curs = connection_to_database()
    # obtengo el listado de partidos
    curs.execute(
        'SELECT id_partido, nombre FROM partido WHERE id_partido != -1')
    rows = curs.fetchall()
    data = {}

    data["Consulta"] = 1
    data["Filas"] = len(rows)
    list_can = []
    for r in rows:
        political_name = r.nombre
        # obtengo el candidato a presidente del partido actual
        curs.execute(
            f'SELECT nombres FROM candidato WHERE id_partido = {r.id_partido} and id_cargo = 1')
        name_president = curs.fetchval()
        curs.execute(
            f'SELECT nombres FROM candidato WHERE id_partido = {r.id_partido} and id_cargo = 2')
        name_vice = curs.fetchval()

        actual = {"Partido": political_name,
                  "Presidente": name_president, "Vicepresidente": name_vice}
        list_can.append(actual)

    data["Return"] = list_can

    curs.close()
    conn.close()
    return data


def consulta2():
    """
    Mostrar el número de candidatos a diputados (esto incluye lista nacional, distrito 
    electoral, parlamento) por cada partido
    """
    conn, curs = connection_to_database()
    # obtener el listado de los partidos inscritos
    curs.execute(
        'SELECT id_partido, nombre FROM partido WHERE id_partido != -1')
    rows = curs.fetchall()
    data = {}
    list_dip = []
    data["Consulta"] = 2
    data["Filas"] = len(rows)

    for r in rows:
        political_name = r.nombre
        # obtener el conteo de candidatos a diputados para el partido
        curs.execute(
            f'SELECT COUNT (*) FROM candidato WHERE id_partido = {r.id_partido} AND (id_cargo >= 3 OR id_cargo<=5)')
        quantity = curs.fetchval()
        actual = {"Partido": political_name, "Cantidad de diputados": quantity}
        list_dip.append(actual)

    data["Return"] = list_dip
    curs.close()
    conn.close()
    return data


def consulta3():
    """
    Mostrar el nombre de los candidatos a alcalde por partido
    """
    conn, curs = connection_to_database()
    # obtener el listado de los partidos inscritos
    curs.execute(
        'SELECT id_partido, nombre FROM partido WHERE id_partido != -1')
    rows = curs.fetchall()
    data = {}
    list_alc = []
    data["Consulta"] = 3
    data["Filas"] = len(rows)

    for r in rows:
        political_name = r.nombre
        # obtener el conteo de candidatos a diputados para el partido
        curs.execute(
            f'SELECT nombres FROM candidato WHERE id_partido = {r.id_partido} AND id_cargo = 6')
        name = curs.fetchval()
        actual = {"Partido": political_name, "Alcalde": name}
        list_alc.append(actual)

    data["Return"] = list_alc
    curs.close()
    conn.close()
    return data
