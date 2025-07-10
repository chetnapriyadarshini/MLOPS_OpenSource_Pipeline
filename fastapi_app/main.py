from fastapi import FastAPI, UploadFile, File
import pandas as pd
from fastapi.responses import HTMLResponse, RedirectResponse
import io

app = FastAPI()
predictions_df = None

@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <h2>Upload predictions via POST to /show-prediction-from-csv</h2>
    <p>Then visit <a href='/show-prediction-table'>/show-prediction-table</a> to view them.</p>
    """

@app.post("/show-prediction-from-csv", response_class=HTMLResponse)
async def upload_predictions(file: UploadFile = File(...)):
    try:
        global predictions_df
        contents = await file.read()
        predictions_df = pd.read_csv(io.BytesIO(contents))
        print("📥 Uploaded CSV:")
        print(predictions_df.head())
        if predictions_df.empty:
            return HTMLResponse(content="<h3>❌ No data found in the uploaded CSV file.</h3>", status_code=400)
        return RedirectResponse(url="/show-prediction-table", status_code=303)
    except Exception as e:
        return HTMLResponse(content=f"<h3>❌ Error reading CSV file: {e}</h3>", status_code=400)
        

@app.get("/show-prediction-table", response_class=HTMLResponse)
def show_predictions():
    if predictions_df is None:
        return "<h3>No predictions uploaded yet.</h3>"
        
    return predictions_df.to_html(index=False, classes="table", border=1)