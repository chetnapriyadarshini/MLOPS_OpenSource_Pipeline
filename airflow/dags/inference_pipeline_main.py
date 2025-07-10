##############################################################################
# Import necessary modules
# #############################################################################

import sys
sys.path.append('/opt/airflow')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from lead_scoring_inference_pipeline import utils
from shared.constants import *
import requests
import sqlite3
import pandas as pd
import os

###############################################################################
# Define default arguments and create an instance of DAG
# ##############################################################################

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022,7,30),
    'retries' : 1, 
    'retry_delay' : timedelta(seconds=5)
}


Lead_scoring_inference_dag = DAG(
                dag_id = 'Lead_scoring_inference_pipeline',
                default_args = default_args,
                description = 'Inference pipeline of Lead Scoring system',
                schedule_interval = '@hourly',
                catchup = False
)

###############################################################################
# Create a task for encode_data_task() function with task_id 'encoding_categorical_variables'
# ##############################################################################
op_encoding_categorical_variables = PythonOperator(task_id='encoding_categorical_variables', 
                                python_callable=utils.encode_features,
                              dag=Lead_scoring_inference_dag)


###############################################################################
# Create a task for load_model() function with task_id 'generating_models_prediction'
# ##############################################################################
op_model_prediction = PythonOperator(task_id='generating_models_prediction', 
                                python_callable=utils.get_models_prediction,
                              dag=Lead_scoring_inference_dag)


###############################################################################
# Create a task for prediction_col_check() function with task_id 'checking_model_prediction_ratio'
# ##############################################################################
op_prediction_ratio_check = PythonOperator(task_id='checking_model_prediction_ratio', 
                                python_callable=utils.prediction_ratio_check,
                              dag=Lead_scoring_inference_dag)


###############################################################################
# Create a task for input_features_check() function with task_id 'checking_input_features'
# ##############################################################################
op_input_features_check = PythonOperator(task_id='checking_input_features', 
                                python_callable=utils.input_features_check,
                              dag=Lead_scoring_inference_dag)

###############################################################################
# Define the FastAPI endpoint to post predictions
###############################################################################

FASTAPI_ENDPOINT = "http://fastapi:8000/show-prediction-from-csv"

# Function to fetch predictions from the DB and post to FastAPI
def post_predictions_to_fastapi():
    try:
        full_db_path = os.path.join(DB_PATH, INFERENCE_CLEAN_DB)
        conn = sqlite3.connect(full_db_path)
        df = pd.read_sql_query(f"SELECT * FROM {PREDICTION}", conn)

        if df.empty:
            print("No predictions to post.")
            return

         # Save predictions to CSV
        date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        csv_file_name = f"predictions_{date}.csv"
        df.to_csv(os.path.join(PREDICTION_PATH, csv_file_name), index=False)
        print(f"✅ Predictions saved to {os.path.join(PREDICTION_PATH, csv_file_name)}")

        # Post CSV to FastAPI
        with open(os.path.join(PREDICTION_PATH, csv_file_name), "rb") as f:
            files = {"file": (csv_file_name, f, "text/csv")}
            response = requests.post(FASTAPI_ENDPOINT, files=files)
        print(f"Posting predictions to FastAPI at {FASTAPI_ENDPOINT}...")

        if response.status_code == 200:
            print("✅ Predictions posted successfully.")
        else:
            print(f"❌ Failed to post predictions. Status code: {response.status_code}, Response: {response.text}")
            raise

    except Exception as e:
        print(f"❌ Error in posting predictions: {e}")
        raise

    finally:
        if conn:
            conn.close()

###############################################################################
# Create a task to post predictions to FastAPI
# ##############################################################################

op_post_predictions = PythonOperator(
    task_id='post_predictions_to_fastapi',
    python_callable=post_predictions_to_fastapi,
    dag=Lead_scoring_inference_dag
)

###############################################################################
# Define relation between tasks
# ##############################################################################

op_encoding_categorical_variables >> op_input_features_check >> op_model_prediction >> op_prediction_ratio_check >> op_post_predictions
