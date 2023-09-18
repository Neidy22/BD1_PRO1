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
    curs.execute('''
                 SELECT p.nombre, c.nombres AS presi, d.nombres AS vice
                 FROM partido p 
                 INNER JOIN candidato c ON p.id_partido = c.id_partido AND c.id_cargo = 1
                 INNER JOIN candidato d ON p.id_partido = d.id_partido AND d.id_cargo = 2
                ''')
    rows = curs.fetchall()
    data = {}

    data["Consulta"] = 1
    data["Filas"] = len(rows)
    list_can = []
    for r in rows:
        actual = {"Partido": r.nombre,
                  "Presidente": r.presi, "Vicepresidente": r.vice}
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

    curs.execute('''
                    SELECT p.nombre, COUNT(c.id_candidato) cantidad
                    FROM partido p
                    INNER JOIN candidato c ON c.id_partido = p.id_partido AND (c.id_cargo > 2 AND c.id_cargo < 6) AND p.id_partido != -1
                    GROUP BY
                        p.nombre

                ''')
    rows = curs.fetchall()
    data = {}
    list_dip = []
    data["Consulta"] = 2
    data["Filas"] = len(rows)

    for r in rows:
        actual = {"Partido": r.nombre, "Cantidad de diputados": r.cantidad}
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
    curs.execute('''
                    SELECT p.nombre, c.nombres
                    FROM partido p
                    INNER JOIN candidato c ON c.id_partido = p.id_partido AND c.id_cargo = 6
                    
                ''')
    rows = curs.fetchall()
    data = {}
    list_alc = []
    data["Consulta"] = 3
    data["Filas"] = len(rows)

    for r in rows:
        actual = {"Partido": r.nombre, "Alcalde": r.nombres}
        list_alc.append(actual)

    data["Return"] = list_alc
    curs.close()
    conn.close()
    return data


def consulta4():
    """
    Cantidad de candidatos por partido (presidentes, vicepresidentes, diputados, 
    alcaldes).
    """
    conn, curs = connection_to_database()
    curs.execute('''
                    SELECT p.nombre, COUNT(c.id_candidato)candidatos
                    FROM partido p
                    INNER JOIN candidato c ON c.id_partido = p.id_partido AND c.id_cargo != -1 AND p.id_partido != -1
                    GROUP BY
                        p.nombre
                ''')
    rows = curs.fetchall()
    data = {}
    list_cand = []
    data["Consulta"] = 4
    data["Filas"] = len(rows)
    for r in rows:
        actual = {"Partido": r.nombre, "Total candidatos": r.candidatos}
        list_cand.append(actual)

    data["Return"] = list_cand
    curs.close()
    conn.close()
    return data


def consulta5():
    """
    Cantidad de votaciones por departamentos.
    """
    conn, curs = connection_to_database()
    curs.execute('''
                    SELECT d.nombre, COUNT(v.id_voto)votos
                    FROM departamento d
                    INNER JOIN mesa m ON m.id_departamento = d.id_departamento
                    INNER JOIN voto v ON v.id_mesa = m.id_mesa
                    GROUP BY d.nombre
                ''')
    rows = curs.fetchall()
    data = {}
    list_dep = []
    data["Consulta"] = 5
    data["Filas"] = len(rows)
    for r in rows:
        actual = {"Departamento": r.nombre, "Total Votos": r.votos}
        list_dep.append(actual)

    data["Return"] = list_dep
    curs.close()
    conn.close()
    return data


def consulta6():
    """
    Cantidad de votos nulos
    """
    conn, curs = connection_to_database()
    data = {}
    data["Consulta"] = 6

    curs.execute('''
                    SELECT COUNT(v.id_voto)
                    FROM voto v
                    INNER JOIN detalle_voto d ON d.id_voto = v.id_voto AND d.id_candidato = -1
                ''')
    quantity = curs.fetchval()
    data["Detalles de votos nulos"] = quantity
    data["Votos nulos"] = quantity/5

    curs.close()
    conn.close()
    return data


def consulta7():
    """
    Top 10 de edad de ciudadanos que realizaron su voto
    """
    conn, curs = connection_to_database()
    data = {}
    list_edad = []
    data["Consulta"] = 7

    curs.execute('''
                SELECT TOP 10 edad, COUNT(edad) cantidad
                FROM 
                    ciudadano
                INNER JOIN voto 
                    ON ciudadano.dpi = voto.dpi
                GROUP BY
                    edad
                ORDER BY cantidad DESC
                ''')
    rows = curs.fetchall()
    for r in rows:
        actual = {"Edad": r.edad, "Cantidad": r.cantidad}
        list_edad.append(actual)
    data["Return"] = list_edad
    curs.close()
    conn.close()
    return data


def consulta8():
    """
        Top 10 de candidatos más votados para presidente y vicepresidente (el voto por 
        presidente incluye el vicepresidente)
    """
    conn, curs = connection_to_database()
    curs.execute('''
                    SELECT TOP 10 p.nombre AS parti, c.nombres AS presi, d.nombres AS vice, COUNT(v.id_candidato)votos
                    FROM  partido p
                    INNER JOIN candidato c ON c.id_partido = p.id_partido AND c.id_cargo = 1
                    INNER JOIN candidato d ON d.id_partido = p.id_partido AND d.id_cargo = 2
                    INNER JOIN detalle_voto v ON v.id_candidato = c.id_candidato
                    GROUP BY p.nombre, c.nombres, d.nombres
                    ORDER BY votos DESC
                ''')
    rows = curs.fetchall()
    data = {}
    lista_cand = []
    data["Consulta"] = 8
    for r in rows:
        actual = {"Partido": r.parti, "Presidente": r.presi,
                  "Vicepresidente": r.vice, "Votos": r.votos}
        lista_cand.append(actual)

    data["Return"] = lista_cand
    curs.close()
    conn.close()
    return data


def consulta9():
    """
        Top 5 de mesas más frecuentadas (mostrar no. Mesa y departamento al que pertenece)
    """
    conn, curs = connection_to_database()

    curs.execute('''
                    SELECT TOP 5 m.id_mesa, d.nombre, COUNT(v.id_voto)votos
                    FROM mesa m
                    INNER JOIN voto v ON v.id_mesa = m.id_mesa
                    INNER JOIN departamento d ON d.id_departamento = m.id_departamento
                    GROUP BY m.id_mesa, d.nombre
                    ORDER BY votos DESC
                ''')
    rows = curs.fetchall()
    data = {}
    list_m = []
    data["Consulta"] = 9
    for r in rows:
        actual = {"No. Mesa": r.id_mesa, "Departamento": r.nombre,
                  "Cantidad de votos": r.votos}
        list_m.append(actual)

    data["Return"] = list_m
    curs.close()
    conn.close()
    return data


def consulta10():
    """
        Mostrar el top 5 la hora más concurrida en que los ciudadanos fueron a votar.
    """
    conn, curs = connection_to_database()
    curs.execute('''
                    SELECT TOP 5 fecha_hora, COUNT(id_voto)cantidad
                    FROM voto
                    GROUP BY
                        fecha_hora
                    ORDER BY
                        cantidad DESC
                ''')

    rows = curs.fetchall()
    data = {}
    top_list = []
    data["Consulta"] = 10

    for r in rows:
        actual = {"Fecha y Hora": r.fecha_hora, "Votos": r.cantidad}
        top_list.append(actual)

    data["Return"] = top_list

    curs.close()
    conn.close()
    return data


def consulta11():
    """
        Cantidad de votos por genero (Masculino, Femenino).
    """
    conn, curs = connection_to_database()

    curs.execute('''
                 SELECT 
                    COUNT(CASE WHEN genero = 'F' THEN ciudadano.dpi END)AS feme,
                    COUNT(CASE WHEN genero = 'M' THEN ciudadano.dpi END)AS masc
                 FROM ciudadano
                 INNER JOIN voto v
                    ON v.dpi = ciudadano.dpi
                ''')

    res = curs.fetchall()
    data = {"Consulta": 11, "Return": [
        {"Genero": "Femenino", "Votos": res[0].feme}, {"Genero": "Masculino", "Votos": res[0].masc}]}
    curs.close()
    conn.close()

    return data
