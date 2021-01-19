from flask import Flask, render_template, url_for, redirect, jsonify, request, Response
from flask_pymongo import PyMongo
from bson import json_util
import numpy as np
import json
# from .scripts.training import _predict



app = Flask(__name__)
app.config["MONGO_URI"] = "mongodb://localhost:27017/deployment"
mongo = PyMongo(app)

# Stackoverflow function to transform the form into a json readable for MongoDB
def parse_multi_form(form):
    dataJson = {}
    for url_k in form:
        v = form[url_k]
        ks = []
        while url_k:
            if '[' in url_k:
                k, r = url_k.split('[', 1)
                ks.append(k)
                if r[0] == ']':
                    ks.append('')
                url_k = r.replace(']', '', 1)
            else:
                ks.append(url_k)
                break
        sub_data = dataJson
        for i, k in enumerate(ks):
            if k.isdigit():
                k = int(k)
            if i+1 < len(ks):
                if not isinstance(sub_data, dict):
                    break
                if k in sub_data:
                    sub_data = sub_data[k]
                else:
                    sub_data[k] = {}
                    sub_data = sub_data[k]
            else:
                if isinstance(sub_data, dict):
                    sub_data[k] = v

    return dataJson

@app.route("/", methods = ["GET"])
def homePage():
    inputs = ['tope_plan_x', 'edad_bene', 'prima', 'anio_incu', 'recorte',
    'pagado_neto', 'linea_negocio_x', 'negocio_x', 'producto_principal',
    'sucursal_x', 'mes_incu', 'gene_afiliado', 'tipo_atencion', 'atencion',
    'tipo_cuadro', 'tx_codi_diag', 'concepto_servicio', 'tx_codi_pres',
    'tx_codi_grci_n1', 'tx_codi_grci_n2', 'tx_codi_grci_n3',
    'agrupacion_diagnostico', 'edad_afiliado','diagnostico', 'plan',
    'anio_fin_cont', 'mes_fin_cont', 'dia_fin_cont', 'anio_ini_cont',
    'mes_ini_cont', 'dia_ini_cont', 'anio_fe_incu', 'mes_fe_incu',
    'dia_fe_incu', 'anio_fe_pago', 'mes_fe_pago', 'dia_fe_pago']
    return render_template("index.html", inputs = inputs)

@app.route("/Data", methods = ['POST'])
def postData():    
    
    dataRequest = parse_multi_form(request.form)
    
    id = mongo.db.data.insert(dataRequest)
    
    data = mongo.db.data.find_one({'_id': id})
    response = json_util.dumps(data)    
    Response(response, mimetype="application/json")

    xJson = json.loads(response)
    xIn = list(xJson.values())
    xIn= xIn[1:]
    print(xIn)

    return 'Hola don Fa'
    



    # return render_template("results.html")

# Modelo
@app.route("/Data", methods = ['GET'])
def getResults():
    
    return render_template("results.html")

@app.route('/Data', methods = ["GET"])
def getData():
    data = mongo.db.data.find()
    response = json_util.dumps(data)
    return Response(response, mimetype="application/json")


if __name__ == "__main__":
    app.run(debug=True)
