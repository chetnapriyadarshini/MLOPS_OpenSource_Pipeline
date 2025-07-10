from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, Optional
from typing import Dict, Union
import pandas as pd
import io
import os
from shared.constants import INDEX_COLUMNS_INFERENCE
import requests

from utils import predict  # You define these

app = FastAPI()

# Optional path to save uploaded files
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "/app/uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

class InputData(BaseModel):
    features: Dict[str, Union[str, float, int]]
    

@app.post("/predict-json")
def predict_from_json(data: InputData):
    try:
       input_dict = data.features
       df = pd.DataFrame([data.features])
        # Ensure the DataFrame has the correct columns
       if not all(col in df.columns for col in INDEX_COLUMNS_INFERENCE):
           raise HTTPException(status_code=400, detail="Input data is missing required columns.")

        # Extract only required columns in specific order
       row = {col: input_dict.get(col, None) for col in INDEX_COLUMNS_INFERENCE}

       df = pd.DataFrame([row])
       res = requests.post("http://data-pipeline:8000/preprocess_data", json=row)
       prediction = predict()
       return {"prediction": prediction}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")

@app.post("/predict-csv")
async def predict_from_csv(
    file: UploadFile = File(...),
    return_file: Optional[bool] = False
    ):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a CSV file.")

    try:
        contents = await file.read()
        df = pd.read_csv(io.StringIO(contents.decode("utf-8")))

        # Save uploaded file (optional)
        save_path = os.path.join(UPLOAD_DIR, file.filename)
        with open(save_path, "wb") as f:
            f.write(contents)
        # Ensure the DataFrame has the correct columns
        if not all(col in df.columns for col in INDEX_COLUMNS_INFERENCE):
            raise HTTPException(status_code=400, detail="Input data is missing required columns.")
        # Extract only required columns in specific order
        df = df[INDEX_COLUMNS_INFERENCE]
        # Convert DataFrame to a list of dictionaries for preprocessing
        rows = df.to_dict(orient="records")
        res = requests.post("http://data-pipeline:8000/preprocess_data", json=rows)

        predictions = predict()
        df["prediction"] = predictions

        if return_file:
            output = io.StringIO()
            df.to_csv(output, index=False)
            output.seek(0)
            return StreamingResponse(output, media_type="text/csv", headers={
                "Content-Disposition": f"attachment; filename=predictions_{file.filename}"
            })

        return JSONResponse(content={"predictions": predictions.tolist()})

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

