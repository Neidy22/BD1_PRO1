import pyodbc
from db.script import CREATE_MODEL_SCRIPT, DELETE_MODEL_SCRIPT
from db.connection import connection_to_server, connection_to_database


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
