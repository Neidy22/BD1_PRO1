# coding=utf-8
from flask import Flask, request
from controllers.temporaryLoad import temporary_bulk_upload

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
