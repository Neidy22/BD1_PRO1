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


@app.route("/consulta4", methods=['GET'])
def query_4():
    res = consulta4()
    return res


@app.route("/consulta5", methods=['GET'])
def query_5():
    res = consulta5()
    return res


@app.route("/consulta6", methods=['GET'])
def query_6():
    res = consulta6()
    return res


@app.route("/consulta7", methods=['GET'])
def query_7():
    res = consulta7()
    return res


@app.route("/consulta8", methods=['GET'])
def query_8():
    res = consulta8()
    return res


@app.route("/consulta9", methods=['GET'])
def query_9():
    res = consulta9()
    return res


@app.route("/consulta10", methods=['GET'])
def query_10():
    res = consulta10()
    return res


@app.route("/consulta11", methods=['GET'])
def query_11():
    res = consulta11()
    return res
