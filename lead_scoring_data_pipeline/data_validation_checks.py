"""
Import necessary modules
############################################################################## 
"""

import pandas as pd
from shared.schema import *
from shared.constants import *
import functools
import sqlite3
from sqlite3 import Error

###############################################################################
# Define function to validate raw data's schema
# ############################################################################## 
CSV_PATH = "/app/input/user_input.csv"
DB_NAME = CLEAN_DB
def raw_data_schema_check():
    '''
    This function check if all the columns mentioned in schema.py are present in
    leadscoring.csv file or not.

   
    INPUTS
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        raw_data_schema : schema of raw data in the form oa list/tuple as present 
                          in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Raw datas schema is in line with the schema present in schema.py' 
        else prints
        'Raw datas schema is NOT in line with the schema present in schema.py'

    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    df_lead_scoring = pd.read_csv(CSV_PATH, index_col=[0])
    if 'app_complete_flag' in df_lead_scoring.columns:
        isInference = False
        print("-------------- TRAINING----------------")
    else:
        isInference = True
        print("-------------- INFERENCE----------------")
    DB_NAME = INFERENCE_CLEAN_DB if isInference else CLEAN_DB
    if functools.reduce(lambda x, y : x and y, map(lambda p, q: p == q,df_lead_scoring.columns,raw_data_schema), True):
        
         print("Raw datas schema is in line with the schema present in schema.py")
    else:
        print(df_lead_scoring.columns)
        print(raw_data_schema)
        print('Raw datas schema is NOT in line with the schema present in schema.py')

###############################################################################
# Define function to validate model's input schema
# ############################################################################## 

def model_input_schema_check():
    '''
    This function check if all the columns mentioned in model_input_schema in 
    schema.py are present in table named in 'model_input' in db file.

   
    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        model_input_schema : schema of models input data in the form oa list/tuple
                          present as in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Models input schema is in line with the schema present in schema.py'
        else prints
        'Models input schema is NOT in line with the schema present in schema.py'
    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    conn = None
    try:
        conn = sqlite3.connect(DB_NAME)
        query = f"SELECT * FROM {MODEL_INPUT_TABLE}"
        model_table = pd.read_sql_query(query, conn)
        if functools.reduce(lambda x, y : x and y, map(lambda p, q: p == q,sorted(model_table.columns),sorted(model_input_schema)), True):       
         print('Models input schema is in line with the schema present in schema.py')
        else:
            print(model_table.columns)
            print(sorted(model_input_schema))
            print('Models input schema is NOT in line with the schema present in schema.py')
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None
    except pd.errors.EmptyDataError:
        print(f"Table '{MODEL_INPUT_TABLE}' is empty.")
        return pd.DataFrame()
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    finally:
        if conn:
            conn.close()
    

    
    
