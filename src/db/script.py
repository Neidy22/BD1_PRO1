TEMP_SCRIPT = """
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
        fecha_hora DATETIME2 NOT NULL
    );
"""

CREATE_MODEL_SCRIPT = """
    CREATE DATABASE pro1;
    USE pro1;
    CREATE TABLE ciudadano
    (
        dpi VARCHAR (13) NOT NULL,
        nombre VARCHAR (50) NOT NULL,
        apellido VARCHAR (50) NOT NULL,
        direccion VARCHAR (100) NOT NULL,
        telefono VARCHAR (10) NOT NULL,
        edad INT NOT NULL,
        genero VARCHAR (1),
        PRIMARY KEY (dpi)
    );

    CREATE TABLE departamento
    (
        id_departamento INT NOT NULL,
        nombre VARCHAR (50) NOT NULL,
        PRIMARY KEY (id_departamento)
    );

    CREATE TABLE partido
    (
        id_partido INT NOT NULL,
        nombre VARCHAR (100) NOT NULL,
        siglas VARCHAR (50) NOT NULL,
        fecha_fundacion DATE NOT NULL,
        PRIMARY KEY (id_partido)
    );

    CREATE TABLE cargo
    (
        id_cargo INT NOT NULL,
        cargo VARCHAR (100) NOT NULL,
        PRIMARY KEY (id_cargo)
    );

    CREATE TABLE mesa
    (
        id_mesa INT NOT NULL,
        id_departamento INT NOT NULL,
        PRIMARY KEY (id_mesa),
        FOREIGN KEY (id_departamento) REFERENCES departamento (id_departamento)
    );

    CREATE TABLE candidato
    (
        id_candidato INT NOT NULL,
        nombres VARCHAR (150) NOT NULL,
        fecha_nacimiento DATE NOT NULL,
        id_partido INT NOT NULL,
        id_cargo INT NOT NULL,
        PRIMARY KEY (id_candidato),
        FOREIGN KEY (id_partido) REFERENCES partido (id_partido),
        FOREIGN KEY (id_cargo) REFERENCES cargo (id_cargo)
    );

    CREATE TABLE voto
    (
        id_voto INT NOT NULL,
        dpi VARCHAR (13) NOT NULL,
        id_mesa INT NOT NULL,
        fecha_hora DATETIME2 NOT NULL,
        PRIMARY KEY (id_voto),
        FOREIGN KEY (dpi) REFERENCES ciudadano (dpi),
        FOREIGN KEY (id_mesa) REFERENCES mesa (id_mesa)
    );

    
    CREATE TABLE detalle_voto
    (
        id_detalle INT PRIMARY KEY IDENTITY(1,1),
        id_voto INT NOT NULL,
        id_candidato INT NOT NULL,
        FOREIGN KEY (id_voto) REFERENCES voto (id_voto),
        FOREIGN KEY (id_candidato) REFERENCES candidato (id_candidato)
    );
"""

DELETE_MODEL_SCRIPT = """
    DROP DATABASE IF EXISTS pro1;
    DROP TABLE IF EXISTS ciudadano;
    DROP TABLE IF EXISTS departamento;
    DROP TABLE IF EXISTS partido;
    DROP TABLE IF EXISTS cargo;
    DROP TABLE IF EXISTS mesa;
    DROP TABLE IF EXISTS candidato;
    DROP TABLE IF EXISTS voto;
    DROP TABLE IF EXISTS detalle_voto;
"""
