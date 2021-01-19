import logging
#from pathlib import Path, PosixPath
#import pandas as pd
#from datetime import date
#from joblib import load

#from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_squared_error, mean_absolute_error
import matplotlib.pyplot as plt


def Evaluate(**kwargs):
    """
    Evaluate the model using the training data. It also logs the metadata of the ML pipeline.
    :return:
    """
    model, x_train, x_test, y_train, y_test=kwargs['ti'].xcom_pull(task_ids='model_point')
    logging.info(f'### kwargs {y_train}')
    predictions = model.predict(x_test)

    logging.info(f'### Mean squared error: {mean_squared_error(y_test, predictions)}')
    logging.info(f'### Mean absolute error: {mean_absolute_error(y_test, predictions)}')
   # logging.info(f'MODELO {model}')
   # logging.info(f'### Feature importances: {model.named_steps["ridgecv"].feature_importances_}')


    plt.scatter(predictions, y_test)
    plt.ylabel('y_test')
    plt.xlabel('prediccion')
    plt.savefig('test.png')

    #x_test.head(1).to_json(r"dato.json")
