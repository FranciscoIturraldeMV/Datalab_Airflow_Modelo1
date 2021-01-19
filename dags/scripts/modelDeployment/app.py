from flask import Flask, render_template, url_for, redirect, jsonify, request, Response
from flask_pymongo import PyMongo
from bson import json_util
# from scripts.training import _predict
from joblib import load



app = Flask(__name__)
app.config["MONGO_URI"] = "mongodb://192.168.77.238:27017/testdb"
mongo = PyMongo(app)

input = ['tope_plan_x', 'edad_bene', 'prima', 'anio_incu', 'recorte',
    'pagado_neto', 'linea_negocio_x', 'negocio_x', 'producto_principal',
    'sucursal_x', 'mes_incu', 'gene_afiliado', 'tipo_atencion', 'atencion',
    'tipo_cuadro', 'tx_codi_diag', 'concepto_servicio', 'tx_codi_pres',
    'tx_codi_grci_n1', 'tx_codi_grci_n2', 'tx_codi_grci_n3',
    'agrupacion_diagnostico', 'edad_afiliado','diagnostico', 'plan',
    'anio_fin_cont', 'mes_fin_cont', 'dia_fin_cont', 'anio_ini_cont',
    'mes_ini_cont', 'dia_ini_cont', 'anio_fe_incu', 'mes_fe_incu',
    'dia_fe_incu', 'anio_fe_pago', 'mes_fe_pago', 'dia_fe_pago']

@app.route("/Data", methods = ['POST'])
def postData():
    tope_plan_x = request.json['tope_plan_x']
    edad_bene = request.json['edad_bene']
    prima = request.json['prima']
    anio_incu= request.json['anio_incu']
    recorte = request.json['recorte']
    pagado_neto = request.json['pagado_neto']
    linea_negocio_x = request.json['linea_negocio_x']
    negocio_x = request.json['negocio_x']
    producto_principal = request.json['producto_principal']
    sucursal_x = request.json['sucursal_x']
    mes_incu = request.json['mes_incu']
    gene_afiliado = request.json['gene_afiliado']
    tipo_atencion = request.json['tipo_atencion']
    atencion = request.json['atencion']
    tipo_cuadro = request.json['tipo_cuadro']
    tx_codi_diag = request.json['tx_codi_diag']
    concepto_servicio = request.json['concepto_servicio']
    tx_codi_pres = request.json['tx_codi_pres']
    tx_codi_grci_n1 = request.json['tx_codi_grci_n1']
    tx_codi_grci_n2 = request.json['tx_codi_grci_n2']
    tx_codi_grci_n3 = request.json['tx_codi_grci_n3']
    agrupacion_diagnostico = request.json['agrupacion_diagnostico']
    edad_afiliado = request.json['edad_afiliado']
    diagnostico = request.json['diagnostico']
    plan = request.json['plan']
    anio_fin_cont = request.json['anio_fin_cont']
    mes_fin_cont = request.json['mes_fin_cont']
    dia_fin_cont = request.json['dia_fin_cont']
    anio_ini_cont = request.json['anio_ini_cont']
    mes_ini_cont = request.json['mes_ini_cont']
    dia_ini_cont = request.json['dia_ini_cont']
    anio_fe_incu = request.json['anio_fe_incu']
    mes_fe_incu = request.json['mes_fe_incu']
    dia_fe_incu = request.json['dia_fe_incu']
    anio_fe_pago = request.json['anio_fe_pago']
    mes_fe_pago = request.json['mes_fe_pago']
    dia_fe_pago = request.json['edad_bene']

    if tope_plan_x and edad_bene and dia_fe_pago and anio_ini_cont and diagnostico:
        id = mongo.db.data.insert(
            {'tope_plan_x': tope_plan_x, 'edad_bene': edad_bene, 'prima': prima, 'anio_incu': anio_incu,
            'recorte': recorte, 'pagado_neto': pagado_neto, 'linea_negocio_x': linea_negocio_x,
            'negocio_x': negocio_x, 'producto_principal': producto_principal, 'sucursal_x': sucursal_x,
            'mes_incu': mes_incu, 'gene_afiliado': gene_afiliado, 'tipo_atencion': tipo_atencion,
            'atencion': atencion, 'tipo_cuadro': tipo_cuadro, 'tx_codi_diag': tx_codi_diag,
            'concepto_servicio': concepto_servicio, 'tx_codi_pres': tx_codi_pres, 'tx_codi_grci_n1': tx_codi_grci_n1,
            'tx_codi_grci_n2': tx_codi_grci_n2, 'tx_codi_grci_n3': tx_codi_grci_n3, 'agrupacion_diagnostico': agrupacion_diagnostico,
            'edad_afiliado': edad_afiliado, 'diagnostico': diagnostico, 'plan': plan, 'anio_fin_cont': anio_fin_cont,
            'mes_fin_cont': mes_fin_cont, 'dia_fin_cont': dia_fin_cont, 'anio_ini_cont': anio_ini_cont,
            'mes_ini_cont': mes_ini_cont, 'dia_ini_cont': dia_ini_cont, 'anio_fe_incu': anio_fe_incu, 
            'mes_fe_incu': mes_fe_incu, 'dia_fe_incu': dia_fe_incu, 'anio_fe_pago': anio_fe_pago,
            'mes_fe_pago': mes_fe_pago, 'dia_fe_pago': dia_fe_pago
            }
        )
        response = {
            'id': str(id),
            'tope_plan_x': tope_plan_x,
            'edad_bene': edad_bene,
            'prima': prima, 
            'anio_incu': anio_incu,
            'recorte': recorte,
            'pagado_neto': pagado_neto, 
            'linea_negocio_x': linea_negocio_x,
            'negocio_x': negocio_x,
            'producto_principal': producto_principal,
            'sucursal_x': sucursal_x,
            'mes_incu': mes_incu,
            'gene_afiliado': gene_afiliado,
            'tipo_atencion': tipo_atencion,
            'atencion': atencion,
            'tipo_cuadro': tipo_cuadro,
            'tx_codi_diag': tx_codi_diag,
            'concepto_servicio': concepto_servicio,
            'tx_codi_pres': tx_codi_pres,
            'tx_codi_grci_n1': tx_codi_grci_n1,
            'tx_codi_grci_n2': tx_codi_grci_n2,
            'tx_codi_grci_n3': tx_codi_grci_n3, 
            'agrupacion_diagnostico': agrupacion_diagnostico,
            'edad_afiliado': edad_afiliado,
            'diagnostico': diagnostico,
            'plan': plan,
            'anio_fin_cont': anio_fin_cont,
            'mes_fin_cont': mes_fin_cont,
            'dia_fin_cont': dia_fin_cont,
            'anio_ini_cont': anio_ini_cont,
            'mes_ini_cont': mes_ini_cont,
            'dia_ini_cont': dia_ini_cont,
            'anio_fe_incu': anio_fe_incu,
            'mes_fe_incu': mes_fe_incu,
            'dia_fe_incu': dia_fe_incu,
            'anio_fe_pago': anio_fe_pago,
            'mes_fe_pago': mes_fe_pago,
            'dia_fe_pago': dia_fe_pago
        }    
        return response
    else:
        return not_found()

@app.route('/data', methods = ["GET"])
def getData():
    data = mongo.db.data.find()
    response = json_util.dumps(data)
    return Response(response, mimetype="application/json")



def _predict(x_test):
  modelo = load('training.pkl')
  return modelo.predict(x_test)

# postData({
#     "tope_plan_x":10000,
#     "edad_bene":48,
#     "prima":4.5208,
#     "anio_incu":2020,
#     "recorte":4.0,
#     "pagado_neto":4.0,
#     "linea_negocio_x":2,
#     "negocio_x":1,
#     "producto_principal":1,
#     "sucursal_x":0,
#     "mes_incu":0,
#     "gene_afiliado":0,
#     "tipo_atencion":0,
#     "atencion":0,
#     "tipo_cuadro":0,
#     "tx_codi_diag":22,
#     "concepto_servicio":6,
#     "tx_codi_pres":6,
#     "tx_codi_grci_n1":8,
#     "tx_codi_grci_n2":14,
#     "tx_codi_grci_n3":19,
#     "agrupacion_diagnostico":3,
#     "edad_afiliado":7,
#     "diagnostico":22,
#     "plan":8,
#     "anio_fin_cont":2021,
#     "mes_fin_cont":6,
#     "dia_fin_cont":18,
#     "anio_ini_cont":2020,
#     "mes_ini_cont":6,
#     "dia_ini_cont":19,
#     "anio_fe_incu":2020,
#     "mes_fe_incu":2,
#     "dia_fe_incu":12,
#     "anio_fe_pago":2020,
#     "mes_fe_pago":4,
#     "dia_fe_pago":14
# })    




if __name__ == "__main__":
    app.run(debug=True)
