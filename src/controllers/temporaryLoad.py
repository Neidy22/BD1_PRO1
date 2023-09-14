import pyodbc
from db.connection import newConnection

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
        siglas VARCHAR (50) NOT NULL
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
        # obtener los scripts de las creaciones de tablas
        sql_temp_scripts = script_temp.split(";")
        # recorrer el arreglo de scripts y crear las tablas
        for script in sql_temp_scripts:
            curs.execute(script)
            msg += "Se creó la tabla temporal \n"
        msg += "Conexión exitosa"

        # llenar las tablas temporales

        # crear las tablas del modelo ya normalizado y con sus respectivas llaves
        # llenar el modelo
        curs.close()  # cierro el cursor
        con.close()  # cierro la conexión

    except pyodbc.Error as e:
        print('Error en carga masiva: ', e)
        msg += "Error en carga masiva: "
        # msg += e

    # finally:
        # cerrar la conexión

    return msg
