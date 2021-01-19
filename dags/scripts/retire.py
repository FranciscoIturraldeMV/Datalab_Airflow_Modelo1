#from pathlib import Path, PosixPath
import numpy as np
import pandas as pd
import logging
import pymongo
from datetime import date
from joblib import dump
from airflow.contrib.hooks.mongo_hook import MongoHook
#import time

def Retire(con, **kwargs):
    """
    Pulls a DataFrame from the heritage of the DAG, and then load it to a MongoDb, with a MongoHook connection.
    """


  #  time.sleep(60)
#Pulling the data
    df =kwargs['ti'].xcom_pull(task_ids='etl_point')
    logging.info(f'### Ok pull {df.shape}')
#Get the collection
    col= con.get_collection(mongo_collection='productos', mongo_db='testdb')
#Delete all
    con.delete_many(mongo_collection='productos', mongo_db='testdb', filter_doc={})
    logging.info(f'### OK drop {col.count()}')

#Transforms DataFrame to JSON by batches
    docs=dict_batch(df, 5000)
    logging.info(f'### Ok cast df to dict {docs}')
#Insert Data to Mongo
    con.insert_many(docs=docs, mongo_collection='productos', mongo_db='testdb')
    logging.info(f'### OK insert {col.count()}')
   # time.sleep(60)

def dict_batch(df: pd.DataFrame, chunk_size: int):
  """
  Transforms a DataFrame to JSON by batches.
  Return: Array of JSON
  """
  dict_arr=[]
  for i in range(df.shape[0]):
    row_temp=df.iloc[i*chunk_size:chunk_size*(i+1),:]
    row_temp=pd.DataFrame(row_temp)
    temp=row_temp.to_dict('records')
    temp=temp[0]
    dict_arr.append(temp)
    logging.info(f'### array de dicts {row_temp}')

    #for the last elements
    if chunk_size*(i+1) >= df.shape[0]:
      logging.info('break')
      break

  return dict_arr
