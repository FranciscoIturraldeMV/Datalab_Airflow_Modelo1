import logging
from pymongo import MongoClient
import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import datetime

from sklearn.decomposition import PCA
from sklearn.linear_model import RidgeCV
from sklearn.pipeline import make_pipeline
from joblib import dump
#import time

SEED = 42

np.random.seed(SEED)

def Training():
  """ 
  Get data using Pymongo, build pipeline, train the model, and export the model with joblib
  Return: model, x and y, test and train.
  """
#Pymongo connection  
  client = MongoClient("mongodb://192.168.77.238:27017")
  logging.info(f'### OK conection')
  #time.sleep(60)
  col = client.testdb.productos
  #logging.info('### OK coleccion, sleeping 60s')
  #time.sleep(60)
  cursor=col.find()
  logging.info(f"### ok extraccion. Length {col.count()}")
  #time.sleep(60)

#Extract the data by chuncks
  data= iterator2dataframes(cursor, 5000)
  logging.info(f'### listo dataframe {data.shape}')
  logging.info(f'{data.info()}')

#Set x and y
  x,y = data.drop(['vlr_cubierto', '_id'], axis=1), data[['vlr_cubierto']]
  logging.info(f'### OK setting x and y {x.head(2), y.head(2)}')
  x_train, x_test, y_train, y_test = train_test_split(x,y,test_size=0.3,random_state=SEED)

#Build the model
  model = _build_ml_pipeline(x_train)
  logging.info(f'### OK building model')
#Fit
  model.fit(x_train, y_train)
 # logging.info(f'### {x.info()}')
  logging.info(f'### Ok fitting {np.sqrt(mean_squared_error(model.predict(x_test),y_test))}')
#Export the model
  dump(model, './training.joblib')
  logging.info('### Modelo exportado')
  #time.sleep(60)
  return model, x_train, x_test, y_train, y_test

def iterator2dataframes(iterator, chunk_size: int):
  """Turn an iterator into multiple small pandas.DataFrame
  This is a balance between memory and efficiency.
  Return: DataFrame
  """
  records = []
  frames = []
  for i, record in enumerate(iterator):
    records.append(record)
    #appending the chuncks
    if i % chunk_size == chunk_size - 1:
      frames.append(pd.DataFrame(records))
     # logging.info(f'append to frames {len(frames)}')
      records = []
  #for the last data
  if records:
    logging.info('### Appending chunks')
    frames.append(pd.DataFrame(records))
    #logging.info(f'haciendo concat {type(frames)}')
    df=frames[0]
    #appending all the chunks to a DataFrame
    for j in range(len(frames)-1):
     # logging.info(f'uniendo {j}')
      df=df.append(frames[j+1])
  logging.info('### lista extraccion y union de datos')
  return df



def _build_ml_pipeline(df: pd.DataFrame):
  """ 
  Builds ML pipeline using sklearn.
  Return: Pipeline
  """ 
  exported_pipeline = make_pipeline(
    PCA(iterated_power=2, svd_solver="randomized"),
    RidgeCV()
  )
  return exported_pipeline
