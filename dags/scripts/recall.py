from pathlib import Path, PosixPath
import numpy as np
import pandas as pd
import logging
from datetime import date
from joblib import dump

from airflow.hooks.postgres_hook import PostgresHook

def Recall():
    src=PostgresHook(postgres_conn_id='dev_postgres')
    src_conn = src.get_conn()
    cursor = src_conn.cursor()
    logging.info(f'### Ok conexion')

    #cursor.execute("SELECT * FROM interaction WHERE id>1617;")
    #src.insert_rows(table='interaction', rows= ((1618, 9999, 9999,10,'bought'))   )
    pand = src.get_pandas_df("SELECT * FROM facturas LIMIT 2;")
    #reemb = src.get_pandas_df("SELECT * FROM ree LIMIT 50;")
    logging.info(f'### INFO DEL CURSOR: {pand}')
    return pand
