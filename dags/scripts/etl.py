#from pathlib import Path, PosixPath
import numpy as np
import pandas as pd
import logging
from datetime import date
from joblib import dump
import time
from sklearn.preprocessing import LabelEncoder
from collections import defaultdict
from airflow.hooks.postgres_hook import PostgresHook

def ETL(con):
    """
    Extract and transform from a Postgres database connection. It uses a PostgresHook connection.
    Returns: Cleaned Data Frame
    """
    logging.info(f'### Ok conexion {con}')

#extract dataframes
    fact = con.get_pandas_df("SELECT * FROM facturas LIMIT 120000;") #78000
    reemb = con.get_pandas_df("SELECT * FROM ree LIMIT 120000;")
    logging.info(f'### Ok dataframes: {fact.shape, reemb.shape}')

    #filtering ambiguous data
    fact = fact[fact['tope_plan'] > 1]
    fact = fact[fact['codigo'] != '-1']
    reemb = reemb[reemb['tope_plan'] > 1]
    reemb = reemb[reemb['codigo'] != '-1']
    logging.info("### Ok limpieza valores atipicos, tope plan y codigo")

#joining two data frames
    df = fact.merge(reemb, on='codigo', how='inner')
    logging.info(f'### Ok merge: {df.shape}')

#transforming
    df_clean = clean_data(df)
    logging.info(f'### Ok transforming {df_clean.shape}')
    #logging.info(f'### A dormir 60s')
    #time.sleep(60)

    return df_clean

def clean_data(df: pd.DataFrame):
  """
  Transforms the data with Pandas.
  Returns: Cleaned DataFrame
  """

  logging.info(f'### Ok eliminando negativos')

#Numeric Casting
  for i in list(["prima", "presentado", "recorte", "deducible", "vlr_gnc", "base_lqui", "vlr_cubierto", "pagado_neto"]):
    df[i]=df[i].str.replace(',',".")

  for i in list(["prima", "presentado", "recorte", "deducible", "vlr_gnc", "base_lqui", "vlr_cubierto", "pagado_neto", 'edad_afiliado']):
    df[i]=df[i].astype('double')


  for i in list(["prima", "presentado", "recorte", "deducible", "vlr_gnc", "base_lqui", "vlr_cubierto", "pagado_neto"]):
    df[i].fillna(0, inplace=True)
  logging.info(f'### OK casting numbers')




# Hot Encoding
  d = defaultdict(LabelEncoder)
  cat=df[["linea_negocio_x", "negocio_x", "producto_principal","sucursal_x",  "mes_incu", "gene_afiliado", "tipo_atencion",
        "atencion", "tipo_cuadro", "tx_codi_diag", "concepto_servicio", "tx_codi_pres",
        "tx_codi_grci_n1", "tx_codi_grci_n2", "tx_codi_grci_n3", "agrupacion_diagnostico", 'edad_afiliado'
        ,'diagnostico', 'plan']].apply(lambda x: d[x.name].fit_transform(x))

  df.drop(["linea_negocio_x", "negocio_x", "producto_principal","sucursal_x",  "mes_incu", "gene_afiliado", "tipo_atencion",
         "atencion", "tipo_cuadro", "tx_codi_diag", "prestador", "concepto_servicio", "tx_codi_pres",
         "tx_codi_grci_n1", "tx_codi_grci_n2", "tx_codi_grci_n3", "agrupacion_diagnostico", 'edad_afiliado',
         'id_contrato_x', 'prestacion', 'nivel_1', 'nivel_2', 'nivel_3', 'diagnostico',
         'codigo', 'transito', 'producto_principal', 'plan'], axis=1, 
        inplace=True)

  logging.info(f"### OK HOT ENCODING")
#Concat the df with the dummies columns
  df_concat = pd.concat([df, cat], axis=1)

  df_concat.dropna(inplace=True)
  logging.info("### OK concating hot encoders")

#To datetime type
  for i in list(['fch_ini_cont_x', 'fch_fin_cont_x', 'fe_incu', 'fe_pago']):
    df_concat[i] = df_concat[i].astype(str)
    df_concat[i] = pd.to_datetime(df_concat[i], errors='coerce')

  df_concat.dropna(inplace=True)
  logging.info("### OK datetime")

#Extract dates
  df_concat['anio_fin_cont']=df_concat['fch_fin_cont_x'].dt.year
  df_concat['mes_fin_cont']=df_concat['fch_fin_cont_x'].dt.month
  df_concat['dia_fin_cont']=df_concat['fch_fin_cont_x'].dt.day

  df_concat['anio_ini_cont']=df_concat['fch_ini_cont_x'].dt.year
  df_concat['mes_ini_cont']=df_concat['fch_ini_cont_x'].dt.month
  df_concat['dia_ini_cont']=df_concat['fch_ini_cont_x'].dt.day

  df_concat['anio_fe_incu']=df_concat['fe_incu'].dt.year
  df_concat['mes_fe_incu']=df_concat['fe_incu'].dt.month
  df_concat['dia_fe_incu']=df_concat['fe_incu'].dt.day

  df_concat['anio_fe_pago']=df_concat['fe_pago'].dt.year
  df_concat['mes_fe_pago']=df_concat['fe_pago'].dt.month
  df_concat['dia_fe_pago']=df_concat['fe_pago'].dt.day

  logging.info("### OK extract dates")

#Set important values
  df_cleaned = df_concat[['tope_plan_x', 'edad_bene',
              'prima', 'anio_incu', 'recorte', 'pagado_neto',
               'linea_negocio_x', 'negocio_x', 'producto_principal', 'sucursal_x', 'mes_incu', 'gene_afiliado',
               'tipo_atencion', 'atencion', 'tipo_cuadro', 'tx_codi_diag', 'concepto_servicio', 'tx_codi_pres',
               'tx_codi_grci_n1', 'tx_codi_grci_n2', 'tx_codi_grci_n3', 'agrupacion_diagnostico', 'edad_afiliado',
               'diagnostico', 'plan', 'anio_fin_cont', 'mes_fin_cont', 'dia_fin_cont', 'anio_ini_cont',
               'mes_ini_cont', 'dia_ini_cont', 'anio_fe_incu', 'mes_fe_incu', 'dia_fe_incu', 'anio_fe_pago',
               'mes_fe_pago', 'dia_fe_pago', 'vlr_cubierto'
              ]]
  return df_cleaned
