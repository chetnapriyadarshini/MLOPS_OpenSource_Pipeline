
import utils
import data_validation_checks
from fastapi import FastAPI, Request
from utils import preprocess_data
import pandas as pd

app = FastAPI()

@app.post("/preprocess_data")
async def run_preprocessing(request: Request):
    data = await request.json()
    df = pd.DataFrame(data)
    
    preprocess_data(df)
    return {"status": "success", "message": "Data preprocessed and saved to DB"}

def run_data_pipeline():
    print("\n[INFO] Starting Data Pipeline...")
    utils.preprocess_data()
    print("[INFO] Data Pipeline Completed.")

if __name__ == "__main__":
    run_data_pipeline()