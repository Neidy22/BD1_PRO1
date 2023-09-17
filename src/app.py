# coding=utf-8
from flask import Flask, request
from controllers.temporary import temporary_bulk_upload, drop_temporary_and_model
from controllers.model import *
import json
app = Flask(__name__)


@app.route("/")
def welcome():
    resp = {
        "message": "La aplicación se ha conectado"
    }
    return resp


@app.route("/cargartabtemp", methods=['GET'])
def bulk_temp():
    msg = temporary_bulk_upload()
    return msg


@app.route("/eliminartabtemp", methods=['GET'])
def delete_temp():
    msg = drop_temporary_and_model()
    return msg


@app.route("/crearmodelo", methods=['GET'])
def create__new_model():
    msg = create_model()
    return msg


@app.route("/consulta1", methods=['GET'])
def query_1():
    res = consulta1()
    return res


@app.route("/consulta2", methods=['GET'])
def query_2():
    res = consulta2()
    return res


@app.route("/consulta3", methods=['GET'])
def query_3():
    res = consulta3()
    return res
