'''
filename: utils.py
functions: encode_features, get_train_model
version: 1
'''

###############################################################################
# Import necessary modules
# ##############################################################################

import pickle
import pandas as pd

import sqlite3
from sqlite3 import Error
import shutil

import mlflow
import mlflow.sklearn
from pycaret.classification import *
from mlflow.models.signature import infer_signature

from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from shared.constants import *
from optuna.distributions import IntDistribution


###############################################################################
# Define the function to encode features
# ##############################################################################

def encode_features():
    '''
    This function one hot encodes the categorical features present in our  
    training dataset. This encoding is needed for feeding categorical data 
    to many scikit-learn models.

    INPUTS
        db_file_name : Name of the database file 
        db_path : path where the db file should be
        ONE_HOT_ENCODED_FEATURES : list of the features that needs to be there in the final encoded dataframe
        FEATURES_TO_ENCODE: list of features  from cleaned data that need to be one-hot encoded
       

    OUTPUT
        1. Save the encoded features in a table - features
        2. Save the target variable in a separate table - target


    SAMPLE USAGE
        encode_features()
        
    '''
    conn = None
    conn2 = None
    try:
        conn = sqlite3.connect(CLEAN_DB)
        conn2 = sqlite3.connect(EXPERIMENT_DB)
        query = f"SELECT * FROM {MODEL_INPUT_TABLE}"
        df = pd.read_sql_query(query, conn)
        target_df = df[TARGET_VAR]
        encoded_df = pd.DataFrame(columns= ONE_HOT_ENCODED_FEATURES)
        placeholder_df = pd.DataFrame()
        
        # One-Hot Encoding using get_dummies for the specified categorical features
        for f in FEATURES_TO_ENCODE:
            if(f in df.columns):
                encoded = pd.get_dummies(df[f])
                encoded = encoded.add_prefix(f + '_')
                placeholder_df = pd.concat([placeholder_df, encoded], axis=1)
            else:
                print('Feature not found')
                
         # Implement these steps to prevent dimension mismatch during inference
        for feature in encoded_df.columns:
            if feature in df.columns:
                encoded_df[feature] = df[feature]
            if feature in placeholder_df.columns:
                encoded_df[feature] = placeholder_df[feature]
                
        # fill all null values
        encoded_df.fillna(0, inplace=True)
        print(encoded_df.columns)
        encoded_df.to_sql(FEATURES_TABLE, conn2, if_exists='replace', index=False)
        target_df.to_sql(TARGET_TABLE, conn2, if_exists='replace', index=False)
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except pd.errors.EmptyDataError:
        print(f"Table '{MODEL_INPUT_TABLE}' is empty.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close() 
        if conn2:
            conn2.close()


###############################################################################
# Define the function to train the model
# ##############################################################################

def get_trained_model():
    '''
    This function setups mlflow experiment to track the run of the training pipeline. It 
    also trains the model based on the features created in the previous function and 
    logs the train model into mlflow model registry for prediction. The input dataset is split
    into train and test data and the auc score calculated on the test data and
    recorded as a metric in mlflow run.   

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be


    OUTPUT
        Tracks the run in experiment named 'Lead_Scoring_Training_Pipeline'
        Logs the trained model into mlflow model registry with name 'LightGBM'
        Logs the metrics and parameters into mlflow run
        Calculate auc from the test data and log into mlflow run  

    SAMPLE USAGE
        get_trained_model()
    '''
    conn = None
     # opening the conncetion for creating the sqlite db
    try:
        conn = sqlite3.connect(EXPERIMENT_DB)

        query = f"SELECT * FROM {FEATURES_TABLE}"
        dataset = pd.read_sql_query(query, conn)
        
        # Optional: downsample dataset for testing
        #dataset = dataset.sample(frac=0.1, random_state=42)

        # Split into training and unseen data
        data_for_model, data_unseen = train_test_split(dataset, test_size=0.2, random_state=786)

        # Reset indexes
        data_for_model.reset_index(drop=True, inplace=True)
        print("Data for model shape:", data_for_model.shape)
        print("Unseen data shape:", data_unseen.shape)
        data_unseen.reset_index(drop=True, inplace=True)

        
        Lead_Scoring_Training_Pipeline = setup(data = data_for_model, target = TARGET_VAR, 
                session_id = 42,fix_imbalance=False,
                n_jobs=-1,use_gpu=True,
                log_experiment=True,experiment_name=EXPERIMENT,
                log_plots=True, log_data=True, verbose=True,
                normalize=False, transformation=False,
                log_profile=False)
        
        lgbm  = create_model('lightgbm', fold = 5)
        custom_grid = {
        'actual_estimator__num_leaves': IntDistribution(10, 100)
        }

        tuned_lgbm_optuna,tuner_1 = tune_model(lgbm, 
                                            search_library='optuna',
                                            fold = 10,
                                            custom_grid=custom_grid,
                                            optimize = 'AUC',
                                            choose_better=True,
                                            return_tuner=True)
        # Finalize and log model
        final_model = finalize_model(tuned_lgbm_optuna)
        # Also save a local copy

        model_path = os.path.join(MODELS_FOLDER, MODEL_NAME)
        if os.path.exists(model_path):
            shutil.rmtree(model_path)

        mlflow.sklearn.save_model(sk_model=final_model, path=model_path)
       # mlflow.log_artifact('LightGBM.pkl')
        target_data = data_for_model[TARGET_VAR]
        data_for_model = data_for_model.drop(columns=[TARGET_VAR])
        # Save columns used for training
        training_columns = data_for_model.columns.tolist()
        with open(os.path.join(MODELS_FOLDER, MODEL_NAME, "training_columns.pkl"), "wb") as f:
            pickle.dump(training_columns, f)
        signature = infer_signature(data_for_model, target_data)
        mlflow.sklearn.log_model(final_model, artifact_path="model", registered_model_name="LightGBM", signature=signature)
        
        predictions = predict_model(final_model, data=data_unseen)
        print("Prediction DataFrame columns:")
        print(predictions.columns.tolist())
        print(predictions.head())
        auc = roc_auc_score(predictions[TARGET_VAR], predictions['prediction_score'])
        mlflow.log_metric("AUC", auc)

        
    # return an error if connection not established
    except Error as e:
        print(e)
     # closing the connection once the database is created
    finally:
        if conn:
            conn.close()
   
